use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::fence;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use concurrent_queue::{ConcurrentQueue, PushError};

/// An `ArcPool` creates `Arc`s whose allocation can be recycled.
///
/// This is intended as a performance optimization, for cases where a lot of time
/// is spent in the memory allocator in `Arc::new` (allocation) and `drop` (deallocation).
///
/// To use this, create one global `ArcPool` per `T`, for example using `OnceLock` as in
/// the example below.
///
/// The pool's internal storage is leaked because we don't want to tie the `Arc`s to the
/// pool's lifetime.
///
/// # Example
///
/// ```
/// use recycling_arc::ArcPool;
///
/// type MyStructArc = recycling_arc::Arc<MyStruct>;
///
/// # #[derive(Default)]
/// # struct MyStruct;
/// fn mystruct_pool() -> &'static ArcPool<MyStruct> {
///     static POOL: std::sync::OnceLock<ArcPool<MyStruct>> = std::sync::OnceLock::new();
///     POOL.get_or_init(|| ArcPool::with_capacity(8))
/// }
///
/// # fn pool_test() {
/// let x = mystruct_pool().make_arc(MyStruct::default());
/// let y = MyStructArc::clone(&x);
/// drop(x);
/// drop(y); // this would "free" the allocation, but with ArcPool it just puts it in the pool.
/// let z = mystruct_pool().make_arc(MyStruct::default()); // this reuses the allocation found in the pool.
/// drop(z);
/// # }
/// ```
pub struct ArcPool<T> {
    /// The pool queue. Allocated on construction and never freed.
    /// XXXmstange: No idea if the type makes sense here. I initially wanted to make this a `&'static PoolQueue<T>` but I didn't want to enforce `T: 'static` (but maybe I should?)
    queue: UnsafeCell<NonNull<PoolQueue<T>>>,
}

unsafe impl<T: Sync + Send> Send for ArcPool<T> {}
unsafe impl<T: Sync + Send> Sync for ArcPool<T> {}

type PoolQueue<T> = ConcurrentQueue<Box<ArcData<T>>>;

impl<T> ArcPool<T> {
    pub fn with_capacity(size: usize) -> Self {
        Self {
            queue: UnsafeCell::new(NonNull::from(Box::leak(Box::new(
                ConcurrentQueue::bounded(size),
            )))),
        }
    }

    pub fn make_arc(&self, value: T) -> Arc<T> {
        let queue = unsafe { (*self.queue.get()).as_ref() };
        match queue.pop() {
            Ok(arc_data) => {
                Arc::<T>::new_with_allocation(value, arc_data, unsafe { *self.queue.get() })
            }
            Err(_) => Arc::<T>::new_with_pool(value, unsafe { *self.queue.get() }),
        }
    }
}

// The Arc code below is from Mara's book, except any of the pool stuff. The pool stuff
// was added by mstange and all the bugs in it are mstange's fault.
//
// https://github.com/m-ou-se/rust-atomics-and-locks/blob/d945e828bd08719a2d7cb6d758be4611bd90ba2b/src/ch6_arc/s3_optimized.rs

pub struct Arc<T> {
    ptr: NonNull<ArcData<T>>,
}

unsafe impl<T: Sync + Send> Send for Arc<T> {}
unsafe impl<T: Sync + Send> Sync for Arc<T> {}

pub struct Weak<T> {
    ptr: NonNull<ArcData<T>>,
}

unsafe impl<T: Sync + Send> Send for Weak<T> {}
unsafe impl<T: Sync + Send> Sync for Weak<T> {}

struct ArcData<T> {
    /// Number of `Arc`s.
    data_ref_count: AtomicUsize,
    /// Number of `Weak`s, plus one if there are any `Arc`s.
    alloc_ref_count: AtomicUsize,
    /// The pool, or None. XXXmstange: No idea if the type makes sense here. I initially wanted to make this a `&'static PoolQueue<T>` but I didn't want to enforce `T: 'static` (but maybe I should?)
    pool: UnsafeCell<Option<NonNull<PoolQueue<T>>>>,
    /// The data. Dropped if there are only weak pointers left.
    data: UnsafeCell<ManuallyDrop<T>>,
}
unsafe impl<T: Sync + Send> Send for ArcData<T> {}
unsafe impl<T: Sync + Send> Sync for ArcData<T> {}

impl<T> Arc<T> {
    pub fn new(data: T) -> Arc<T> {
        Arc {
            ptr: NonNull::from(Box::leak(Box::new(ArcData {
                alloc_ref_count: AtomicUsize::new(1),
                data_ref_count: AtomicUsize::new(1),
                pool: UnsafeCell::new(None),
                data: UnsafeCell::new(ManuallyDrop::new(data)),
            }))),
        }
    }

    fn new_with_pool(data: T, pool: NonNull<PoolQueue<T>>) -> Arc<T> {
        Arc {
            ptr: NonNull::from(Box::leak(Box::new(ArcData {
                alloc_ref_count: AtomicUsize::new(1),
                data_ref_count: AtomicUsize::new(1),
                pool: UnsafeCell::new(Some(pool)),
                data: UnsafeCell::new(ManuallyDrop::new(data)),
            }))),
        }
    }

    fn new_with_allocation(
        data: T,
        mut arc_data: Box<ArcData<T>>,
        pool: NonNull<PoolQueue<T>>,
    ) -> Arc<T> {
        *arc_data = ArcData {
            alloc_ref_count: AtomicUsize::new(1),
            data_ref_count: AtomicUsize::new(1),
            pool: UnsafeCell::new(Some(pool)),
            data: UnsafeCell::new(ManuallyDrop::new(data)),
        };
        Arc {
            ptr: NonNull::from(Box::leak(arc_data)),
        }
    }

    fn data(&self) -> &ArcData<T> {
        unsafe { self.ptr.as_ref() }
    }

    pub fn get_mut(arc: &mut Self) -> Option<&mut T> {
        // Acquire matches Weak::drop's Release decrement, to make sure any
        // upgraded pointers are visible in the next data_ref_count.load.
        if arc
            .data()
            .alloc_ref_count
            .compare_exchange(1, usize::MAX, Acquire, Relaxed)
            .is_err()
        {
            return None;
        }
        let is_unique = arc.data().data_ref_count.load(Relaxed) == 1;
        // Release matches Acquire increment in `downgrade`, to make sure any
        // changes to the data_ref_count that come after `downgrade` don't
        // change the is_unique result above.
        arc.data().alloc_ref_count.store(1, Release);
        if !is_unique {
            return None;
        }
        // Acquire to match Arc::drop's Release decrement, to make sure nothing
        // else is accessing the data.
        fence(Acquire);
        unsafe { Some(&mut *arc.data().data.get()) }
    }

    pub fn downgrade(arc: &Self) -> Weak<T> {
        let mut n = arc.data().alloc_ref_count.load(Relaxed);
        loop {
            if n == usize::MAX {
                std::hint::spin_loop();
                n = arc.data().alloc_ref_count.load(Relaxed);
                continue;
            }
            assert!(n <= usize::MAX / 2);
            // Acquire synchronises with get_mut's release-store.
            if let Err(e) =
                arc.data()
                    .alloc_ref_count
                    .compare_exchange_weak(n, n + 1, Acquire, Relaxed)
            {
                n = e;
                continue;
            }
            return Weak { ptr: arc.ptr };
        }
    }
}

impl<T> Deref for Arc<T> {
    type Target = T;

    fn deref(&self) -> &T {
        // Safety: Since there's an Arc to the data,
        // the data exists and may be shared.
        unsafe { &*self.data().data.get() }
    }
}

impl<T> Weak<T> {
    fn data(&self) -> &ArcData<T> {
        unsafe { self.ptr.as_ref() }
    }

    pub fn upgrade(&self) -> Option<Arc<T>> {
        let mut n = self.data().data_ref_count.load(Relaxed);
        loop {
            if n == 0 {
                return None;
            }
            assert!(n <= usize::MAX / 2);
            if let Err(e) =
                self.data()
                    .data_ref_count
                    .compare_exchange_weak(n, n + 1, Relaxed, Relaxed)
            {
                n = e;
                continue;
            }
            return Some(Arc { ptr: self.ptr });
        }
    }
}

impl<T> Clone for Weak<T> {
    fn clone(&self) -> Self {
        if self.data().alloc_ref_count.fetch_add(1, Relaxed) > usize::MAX / 2 {
            std::process::abort();
        }
        Weak { ptr: self.ptr }
    }
}

impl<T> Drop for Weak<T> {
    fn drop(&mut self) {
        if self.data().alloc_ref_count.fetch_sub(1, Release) == 1 {
            fence(Acquire);
            unsafe {
                let data = Box::from_raw(self.ptr.as_ptr());
                match &*data.pool.get() {
                    Some(pool) => match pool.as_ref().push(data) {
                        Ok(()) => {
                            // The allocation got put in the pool.
                        }
                        Err(PushError::Full(data) | PushError::Closed(data)) => {
                            // The pool was full.
                            drop(data)
                        }
                    },
                    None => drop(data),
                }
            }
        }
    }
}

impl<T> Clone for Arc<T> {
    fn clone(&self) -> Self {
        if self.data().data_ref_count.fetch_add(1, Relaxed) > usize::MAX / 2 {
            std::process::abort();
        }
        Arc { ptr: self.ptr }
    }
}

impl<T> Drop for Arc<T> {
    fn drop(&mut self) {
        if self.data().data_ref_count.fetch_sub(1, Release) == 1 {
            fence(Acquire);
            // Safety: The data reference counter is zero,
            // so nothing will access the data anymore.
            unsafe {
                ManuallyDrop::drop(&mut *self.data().data.get());
            }
            // Now that there's no `Arc<T>`s left,
            // drop the implicit weak pointer that represented all `Arc<T>`s.
            drop(Weak { ptr: self.ptr });
        }
    }
}
