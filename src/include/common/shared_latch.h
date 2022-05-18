#pragma once

// DEPRECATED
// #include <tbb/reader_writer_lock.h>
#include <shared_mutex>
#include "common/macros.h"

namespace terrier::common {

/**
 * A cheap and easy shared (reader-writer) latch, currently wraps tbb::reader_writer_lock. From Intel's docs:
 *
 * A reader_writer_lock is scalable and nonrecursive. The implementation handles lock requests on a first-come
 * first-serve basis except that writers have preference over readers. Waiting threads busy wait, which can degrade
 * system performance if the wait is long. However, if the wait is typically short, a reader_writer_lock can provide
 * performance competitive with other mutexes.
 */
class SharedLatch {
 public:
  /**
   * Acquire exclusive lock on mutex.
   */
  void LockExclusive() { latch_.lock(); }

  /**
   * Acquire shared lock on mutex.
   */
  // DEPRECATED
  // void LockShared() { latch_.lock_read(); }
  void LockShared() { latch_.lock_shared(); }

  /**
   * Try to acquire exclusive lock on mutex.
   * @return true if lock acquired, false otherwise.
   */
  bool TryExclusiveLock() { return latch_.try_lock(); }

  /**
   * Try to acquire shared lock on mutex.
   * @return true if lock acquired, false otherwise.
   */
  // DEPRECATED
  // bool TryLockShared() { return latch_.try_lock_read(); }
  bool TryLockShared() { return latch_.try_lock_shared(); }

  /**
   * Release lock.
   */
  void Unlock() { latch_.unlock(); }

  /**
   * Scoped read latch that guarantees releasing the latch when destructed.
   */
  class ScopedSharedLatch {
   public:
    /**
     * Acquire write lock on ReaderWriterLatch.
     * @param rw_latch pointer to ReaderWriterLatch to acquire
     */
    explicit ScopedSharedLatch(SharedLatch *const rw_latch) : rw_latch_(rw_latch) { rw_latch_->LockShared(); }
    /**
     * Release write lock (if acquired).
     */
    ~ScopedSharedLatch() { rw_latch_->Unlock(); }
    DISALLOW_COPY_AND_MOVE(ScopedSharedLatch)

   private:
    SharedLatch *const rw_latch_;
  };

  /**
   * Scoped write latch that guarantees releasing the latch when destructed.
   */
  class ScopedExclusiveLatch {
   public:
    /**
     * Acquire read lock on ReaderWriterLatch.
     * @param rw_latch pointer to ReaderWriterLatch to acquire
     */
    explicit ScopedExclusiveLatch(SharedLatch *const rw_latch) : rw_latch_(rw_latch) { rw_latch_->LockExclusive(); }
    /**
     * Release read lock (if acquired).
     */
    ~ScopedExclusiveLatch() { rw_latch_->Unlock(); }
    DISALLOW_COPY_AND_MOVE(ScopedExclusiveLatch)
   private:
    SharedLatch *const rw_latch_;
  };

 private:
  // DEPRECATED
  // tbb::reader_writer_lock latch_;
  std::shared_mutex latch_;
};

}  // namespace terrier::common
