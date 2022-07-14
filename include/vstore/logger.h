//
// Created by zhangqian on 2021/11/1.
//

//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.h
//
// Identification: src/include/logging/log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>
#include <memory>
#include <thread>
#include <vector>

#include "../common/concurrent_blocking_queue.h"
#include "../common/concurrent_queue.h"
#include "../common/dedicated_thread_owner.h"
#include "../common/dedicated_thread_registry.h"
#include "../common/dedicated_thread_task.h"
#include "../common/logger.h"
#include "../common/macros.h"
#include "log_io.h"
#include "valen_buffer.h"

namespace mvstore {

//===--------------------------------------------------------------------===//
// wal log Manager
//===--------------------------------------------------------------------===//

class WalLogManager;  // forward declaration

/**
 * A RedoBuffer is a fixed-sized buffer to hold RedoRecords.
 *
 * Not thread-safe
 */
class RedoBuffer {
public:
    /**
     * Initializes a new RedoBuffer, working with the given LogManager
     * @param log_manager the log manager this redo buffer talks to, or nullptr if logging is disabled
     * @param buffer_pool The buffer pool to draw buffer segments from. Must be the same buffer pool the log manager uses.
     */
    RedoBuffer(WalLogManager *log_manager, RecordBufferPool *buffer_pool)
            : has_flushed_(false), log_manager_(log_manager), buffer_pool_(buffer_pool) {}

    /**
     * Reserve a redo record with the given size, in bytes. The returned pointer is guaranteed to be valid until NewEntry
     * is called again, or when the buffer is explicitly flushed by the call Finish().
     * @param size the size of the redo record to allocate
     * @return a new redo record with at least the given size reserved
     */
    char *NewEntry(uint32_t size);

    /**
     * Flush all contents of the redo buffer to be logged out, effectively closing this redo buffer. No further entries
     * can be written to this redo buffer after the function returns.
     * @param flush_buffer whether the transaction holding this RedoBuffer should flush the its redo buffer
     */
    void Finalize(bool flush_buffer);

    /**
     * @return a pointer to the beginning of the last record requested, or nullptr if no record exists.
     */
    char *LastRecord() const { return last_record_; }

    /**
     * @return true if this buffer has previously flushed to the log manager
     */
    bool HasFlushed() const { return has_flushed_; }

    /**
     * Reset the RedoBuffer to empty
     */
    void Reset() {
        if (buffer_seg_ != nullptr) buffer_seg_->Reset();
    }

private:
    // Flag to denote if this RedoBuffer has flushed records to the log manager already.
    // We use this to determine if we should write an abort record, since we only need to write an abort record if this
    // buffer has previously flushed logs to the log manager. In the case of recovery, the abort record helps it discard
    // changes from aborted txns
    bool has_flushed_;
    WalLogManager *const log_manager_;
    RecordBufferPool *const buffer_pool_;
    ValenBuffer *buffer_seg_ = nullptr;
    // reserved for aborts where we will potentially need to garbage collect the last operation (which caused the abort)
    char *last_record_ = nullptr;
};



/**
 * A thin wrapper around a buffer segment to allow iteration through its contents as the given template type
 * @tparam RecordType records are treated as this type when iterating. Must expose an instance method Size() that
 *                    gives the size of the record in memory in bytes
 */
template <class RecordType>
class IterableBuffer {
public:
    /**
     * Iterator for iterating through the records in the buffer
     */
    class Iterator {
    public:
        /**
         * @return reference to the underlying record
         */
        RecordType &operator*() const { return *reinterpret_cast<RecordType *>(segment_->buffer_data_ + segment_offset_); }

        /**
         * @return pointer to the underlying record
         */
        RecordType *operator->() const { return reinterpret_cast<RecordType *>(segment_->buffer_data_ + segment_offset_); }

        /**
         * prefix increment
         * @return self-reference
         */
        Iterator &operator++() {
            RecordType &me = this->operator*();
            segment_offset_ += me.Size();
            return *this;
        }

        /**
         * postfix increment
         * @return iterator that is equal to this before increment
         */
        Iterator operator++(int) {
            Iterator copy = *this;
            operator++();
            return copy;
        }

        /**
         * equality check
         * @param other the other iterator to compare to
         * @return if the two iterators point to the same underlying record
         */
        bool operator==(const Iterator &other) const {
            return segment_offset_ == other.segment_offset_ && segment_ == other.segment_;
        }

        /**
         * inequality check
         * @param other the other iterator to comapre to
         * @return if the two iterators point to different underlying records
         */
        bool operator!=(const Iterator &other) const { return !(*this == other); }

    private:
        friend class IterableBuffer;
        Iterator(ValenBuffer *segment, uint32_t segment_offset)
                : segment_(segment), segment_offset_(segment_offset) {}
        ValenBuffer *segment_;
        uint32_t segment_offset_;
    };

    /**
     * Instantiates an IterableBufferSegment as a wrapper around a buffer segment
     * @param segment
     */
    explicit IterableBuffer(ValenBuffer *segment) : segment_(segment) {}

    /**
     * @return iterator to the first element
     */
    Iterator begin() { return {segment_, 0}; }  // NOLINT for STL name compability

    /**
     * @return iterator to the second element
     */
    Iterator end() { return {segment_, segment_->size_}; }  // NOLINT for STL name compability

private:
    ValenBuffer *segment_;
};

class LogSerializerTask : public DedicatedThreadTask {
public:
    /**
     * @param serialization_interval Interval time for when to trigger serialization
     * @param buffer_pool buffer pool to use to release serialized buffers
     * @param empty_buffer_queue pointer to queue to pop empty buffers from
     * @param filled_buffer_queue pointer to queue to push filled buffers to
     * @param disk_log_writer_thread_cv pointer to condition variable to notify consumer when a new buffer has handed over
     */
    explicit LogSerializerTask(const std::chrono::microseconds serialization_interval,
                               RecordBufferPool *buffer_pool,
                               ConcurrentBlockingQueue<BufferedLogWriter *> *empty_buffer_queue,
                               ConcurrentQueue<BufferedLogWriter *> *filled_buffer_queue,
                               std::condition_variable *disk_log_writer_thread_cv)
            : run_task_(false),
              serialization_interval_(serialization_interval),
              buffer_pool_(buffer_pool),
              filled_buffer_(nullptr),
              empty_buffer_queue_(empty_buffer_queue),
              filled_buffer_queue_(filled_buffer_queue),
              disk_log_writer_thread_cv_(disk_log_writer_thread_cv) {}

    /**
     * Runs main disk log writer loop. Called by thread registry upon initialization of thread
     */
    void RunTask() override {
        run_task_ = true;
        LogSerializerTaskLoop();
    }

    /**
     * Signals task to stop. Called by thread registry upon termination of thread
     */
    void Terminate() override {
        // If the task hasn't run yet, yield the thread until it's started
        while (!run_task_) std::this_thread::yield();
        PELOTON_ASSERT(run_task_, "log serialize task run task terminate.");
        run_task_ = false;
    }

    /**
     * Hands a (possibly partially) filled buffer to the serializer task to be serialized
     * @param buffer_segment the (perhaps partially) filled log buffer ready to be consumed
     */
    void AddBufferToFlushQueue(ValenBuffer *const buffer_segment) {
        {
            std::unique_lock<std::mutex> guard(flush_queue_latch_);
            flush_queue_.push(buffer_segment);
            empty_ = false;
            if (sleeping_) flush_queue_cv_.notify_all();
        }
    }

private:
    // Flag to signal task to run or stop
    bool run_task_;
    // Interval for serialization
    const std::chrono::microseconds serialization_interval_;

    // Used to release processed buffers
    RecordBufferPool *buffer_pool_;

    // Ensures only one thread is serializing at a time.
    SpinLatch serialization_latch_;

    // TODO(Tianyu): Might not be necessary, since commit on txn manager is already protected with a latch
    // TODO(Tianyu): benchmark for if these should be concurrent data structures, and if we should apply the same
    //  optimization we applied to the GC queue.
    // Latch to protect flush queue
    std::mutex flush_queue_latch_;
    // Stores unserialized buffers handed off by transactions
    std::queue<ValenBuffer *> flush_queue_;

    // conditional variable to be notified when there are logs to be processed
    std::condition_variable flush_queue_cv_;

    // bools representing whether the logging thread is sleeping and if the log queue is empty
    bool sleeping_ = false, empty_ = true;

    // Current buffer we are serializing logs to
    BufferedLogWriter *filled_buffer_;
    // Commit callbacks for commit records currently in filled_buffer
//    std::vector<std::pair<concurrency::callback_fn, void *>> commits_in_buffer_;

    // Used by the serializer thread to store buffers it has grabbed from the log manager
    std::queue<ValenBuffer *> temp_flush_queue_;

    // We aggregate all transactions we serialize so we can bulk remove the from the timestamp manager
    // TODO(Gus): If we guarantee there is only one TSManager in the system, this can just be a vector. We could also pass
    // TS into the serializer instead of having a pointer for it in every commit/abort record
//  std::unordered_map<transaction::TimestampManager *, std::vector<transaction::timestamp_t>> serialized_txns_;
//    std::vector<concurrency::TransactionContext *> serialized_txns_;
    // The queue containing empty buffers. Task will dequeue a buffer from this queue when it needs a new buffer
    ConcurrentBlockingQueue<BufferedLogWriter *> *empty_buffer_queue_;
    // The queue containing filled buffers. Task should push filled serialized buffers into this queue
//    ConcurrentQueue<SerializedLogs> *filled_buffer_queue_;
    ConcurrentQueue<BufferedLogWriter *> *filled_buffer_queue_;

    // Condition variable to signal disk log consumer task thread that a new full buffer has been pushed to the queue
    std::condition_variable *disk_log_writer_thread_cv_;

    /**
     * Main serialization loop. Calls Process every interval. Processes all the accumulated log records and
     * serializes them to log consumer tasks.
     */
    void LogSerializerTaskLoop();

    /**
     * Process all the accumulated log records and serialize them to log consumer tasks. It's important that we serialize
     * the logs in order to ensure that a single transaction's logs are ordered. Only a single thread can serialize the
     * logs (without more sophisticated ordering checks).
     * @return (number of bytes processed, number of records processed, number of transactions processed)
     */
    std::tuple<uint64_t, uint64_t, uint64_t> Process();

    /**
     * Serialize out the task buffer to the current serialization buffer
     * @param buffer_to_serialize the iterator to the redo buffer to be serialized
     * @return tuple representing number of bytes, number of records, and number of txns serialized, used for metrics
     */
    std::tuple<uint64_t, uint64_t, uint64_t> SerializeBuffer(IterableBuffer<LogRecord> *buffer_to_serialize);

    /**
     * Serialize out the record to the log
     * @param record the redo record to serialise
     * @return bytes serialized, used for metrics
     */
    uint64_t SerializeRecord(const LogRecord &record);

    /**
     * Serialize the data pointed to by val to current serialization buffer
     * @tparam T Type of the value
     * @param val The value to write to the buffer
     * @return bytes written, used for metrics
     */
    template <class T>
    uint32_t WriteValue(const T &val) {
        return WriteValue(&val, sizeof(T));
    }

    /**
     * Serialize the data pointed to by val to current serialization buffer
     * @param val the value
     * @param size size of the value to serialize
     * @return bytes written, used for metrics
     */
    uint32_t WriteValue(const void *val, uint32_t size);

    /**
     * Returns the current buffer to serialize logs to
     * @return buffer to write to
     */
    BufferedLogWriter *GetCurrentWriteBuffer();

    /**
     * Hand over the current buffer and commit callbacks for commit records in that buffer to the log consumer task
     */
    void HandFilledBufferToWriter();
};

class DiskLogConsumerTask : public DedicatedThreadTask {
public:
    /**
     * Constructs a new DiskLogConsumerTask
     * @param persist_interval Interval time for when to persist log file
     * @param persist_threshold threshold of data written since the last persist to trigger another persist
     * @param buffers pointer to list of all buffers used by log manager, used to persist log file
     * @param empty_buffer_queue pointer to queue to push empty buffers to
     * @param filled_buffer_queue pointer to queue to pop filled buffers from
     */
    explicit DiskLogConsumerTask(const std::chrono::microseconds persist_interval, uint64_t persist_threshold,
                                 std::vector<BufferedLogWriter> *buffers,
                                 ConcurrentBlockingQueue<BufferedLogWriter *> *empty_buffer_queue,
                                 ConcurrentQueue<BufferedLogWriter *> *filled_buffer_queue)
            : run_task_(false),
              persist_interval_(persist_interval),
              persist_threshold_(persist_threshold),
              current_data_written_(0),
              buffers_(buffers),
              empty_buffer_queue_(empty_buffer_queue) ,
              filled_buffer_queue_(filled_buffer_queue) {}

    /**
     * Runs main disk log writer loop. Called by thread registry upon initialization of thread
     */
    void RunTask() override;

    /**
     * Signals task to stop. Called by thread registry upon termination of thread
     */
    void Terminate() override;

private:
    friend class WblLogManager;
    // Flag to signal task to run or stop
    bool run_task_;
    // Stores callbacks for commit records written to disk but not yet persisted
//    std::vector<CommitCallback> commit_callbacks_;

    // Interval time for when to persist log file
    const std::chrono::microseconds persist_interval_;
    // Threshold of data written since the last persist to trigger another persist
    uint64_t persist_threshold_;
    // Amount of data written since last persist
    uint64_t current_data_written_;

    // This stores a reference to all the buffers the log manager has created. Used for persisting
    std::vector<BufferedLogWriter> *buffers_;
    // The queue containing empty buffers. Task will enqueue a buffer into this queue when it has flushed its logs
    ConcurrentBlockingQueue<BufferedLogWriter *> *empty_buffer_queue_;
    // The queue containing filled buffers. Task should dequeue filled buffers from this queue to flush
//    ConcurrentQueue<SerializedLogs> *filled_buffer_queue_;
    ConcurrentQueue<BufferedLogWriter *> *filled_buffer_queue_;

    // Flag used by the serializer thread to signal the disk log consumer task thread to persist the data on disk
    volatile bool force_flush_;

    // Synchronisation primitives to synchronise persisting buffers to disk
    std::mutex persist_lock_;
    std::condition_variable persist_cv_;
    // Condition variable to signal disk log consumer task thread to wake up and flush buffers to disk or if shutdown has
    // initiated, then quit
    std::condition_variable disk_log_writer_thread_cv_;

    /**
     * Main disk log consumer task loop. Flushes buffers to disk when new buffers are handed to it via
     * filled_buffer_queue_, or when notified by LogManager to persist buffers
     */
    void DiskLogConsumerTaskLoop();

    /**
     * Flush all buffers in the filled buffers queue to the log file
     */
    void WriteBuffersToLogFile();

    /*
     * Persists the log file on disk by calling fsync, as well as calling callbacks for all committed transactions that
     * were persisted
     * @return number of buffers persisted, used for metrics
     */
    uint64_t PersistLogFile();
};
/**
* A LogManager is responsible for serializing log records out and keeping track of whether changes from a transaction
* are persistent. The standard flow of a log record from a transaction all the way to disk is as follows:
*      1. The LogManager receives buffers containing records from transactions via the AddBufferToFlushQueue, and
* adds them to the serializer task's flush queue (flush_queue_)
*      2. The LogSerializerTask will periodically process and serialize buffers in its flush queue
* and hand them over to the consumer queue (filled_buffer_queue_). The reason this is done in the background and not as
* soon as logs are received is to reduce the amount of time a transaction spends interacting with the log manager
*      3. When a buffer of logs is handed over to a consumer, the consumer will wake up and process the logs. In the
* case of the DiskLogConsumerTask, this means writing it to the log file.
*      4. The DiskLogConsumer task will persist the log file when:
*          a) Someone calls ForceFlush on the LogManager, or
*          b) Periodically
*          c) A sufficient amount of data has been written since the last persist
*      5. When the persist is done, the `DiskLogConsumerTask` will call the commit callbacks for any CommitRecords that
* were just persisted.
*/
class WblLogManager : public DedicatedThreadOwner {
public:
    /**
  * Constructs a new LogManager, writing its logs out to the given file.
  *
  * @param log_file_path path to the desired log file location. If the log file does not exist, one will be created;
  *                      otherwise, changes are appended to the end of the file.
  * @param num_buffers Number of buffers to use for buffering logs
  * @param serialization_interval Interval time between log serializations
  * @param persist_interval Interval time between log flushing
  * @param persist_threshold data written threshold to trigger log file persist
  * @param buffer_pool the object pool to draw log buffers from. This must be the same pool transactions draw their
  *                    buffers from
  * @param thread_registry DedicatedThreadRegistry dependency injection
  */
    WblLogManager(std::string log_file_path, uint64_t num_buffers, std::chrono::microseconds serialization_interval,
                  std::chrono::microseconds persist_interval, uint64_t persist_threshold,
                  RecordBufferPool *buffer_pool)
            : run_log_manager_(false),
              log_file_path_(std::move(log_file_path)),
              num_buffers_(num_buffers),
              buffer_pool_(buffer_pool),
              serialization_interval_(serialization_interval),
              persist_interval_(persist_interval),
              persist_threshold_(persist_threshold) {}
    /**
  * Starts log manager. Does the following in order:
  *    1. Initialize buffers to pass serialized logs to log consumers
  *    2. Starts up DiskLogConsumerTask
  *    3. Starts up LogSerializerTask
  */
    void Start();

    /**
  * Serialize and flush the logs to make sure all serialized records are persistent. Callbacks from committed
  * transactions are invoked by log consumers when the commit records are persisted on disk.
  * @warning This method should only be called from a dedicated flushing thread or during testing
  * @warning Beware the performance consequences of calling flush too frequently
  */
    void ForceFlush();

    /**
  * Persists all unpersisted logs and stops the log manager. Does what Start() does in reverse order:
  *    1. Stops LogSerializerTask
  *    2. Stops DiskLogConsumerTask
  *    3. Closes all open buffers
  * @note Start() can be called to run the log manager again, a new log manager does not need to be initialized.
  */
    void PersistAndStop();

    /**
  * Returns a (perhaps partially) filled log buffer to the log manager to be consumed. Caller should drop its
  * reference to the buffer after the method returns immediately, as it would no longer be safe to read from or
  * write to the buffer. This method can be called safely from concurrent execution threads.
  *
  * @param buffer_segment the (perhaps partially) filled log buffer ready to be consumed
  */
    void AddBufferToFlushQueue(ValenBuffer *buffer_segment);

    /**
  * For testing only
  * @return number of buffers used for logging
  */
    uint64_t TestGetNumBuffers() { return num_buffers_; }

    /**
      * Set the number of buffers used for buffering logs.
      * The operation fails if the LogManager has already allocated more
      * buffers than the new size
      *
      * @param new_num_buffers the new number of buffers the log manager can use
      * @return true if new_num_buffers is successfully set and false the operation fails
      */
    bool SetNumBuffers(uint64_t new_num_buffers) {
        if (new_num_buffers >= num_buffers_) {
            // Add in new buffers
            for (size_t i = 0; i < new_num_buffers - num_buffers_; i++) {
                buffers_.emplace_back(BufferedLogWriter(log_file_path_.c_str()));
                empty_buffer_queue_.Enqueue(&buffers_[num_buffers_ + i]);
            }
            num_buffers_ = new_num_buffers;
            return true;
        }
        return false;
    }

// protected:
//  /**
//* If the central registry wants to removes our thread used for the disk log consumer task, we only allow removal if
//* we are in shut down, else we need to keep the task, so we reject the removal
//* @return true if we allowed thread to be removed, else false
//*/
//  bool OnThreadRemoved(std::shared_ptr<DedicatedThreadTask> task) override{
//    // We don't want to register a task if the log manager is shutting down though.
//    return !run_log_manager_;
//  }

private:
    // Flag to tell us when the log manager is running or during termination
    bool run_log_manager_;

    // System path for log file
    std::string log_file_path_;

    // Number of buffers to use for buffering and serializing logs
    uint64_t num_buffers_;

    // TODO(Tianyu): This can be changed later to be include things
    //  that are not necessarily backed by a disk
    //  (e.g. logs can be streamed out to the network for remote replication)
    RecordBufferPool *buffer_pool_;

    // This stores a reference to all the buffers the serializer or the log consumer threads use
    std::vector<BufferedLogWriter> buffers_;
    // The queue containing empty buffers which the serializer thread will use. We use a blocking queue because the
    // serializer thread should block when requesting a new buffer until it receives an empty buffer
    ConcurrentBlockingQueue<BufferedLogWriter *> empty_buffer_queue_;
    // The queue containing filled buffers pending flush to the disk
//    ConcurrentQueue<SerializedLogs> filled_buffer_queue_;
    ConcurrentQueue<BufferedLogWriter *> filled_buffer_queue_;

    // Log serializer task that processes buffers handed over by transactions and serializes them into consumer buffers
    std::shared_ptr<LogSerializerTask> log_serializer_task_ = std::shared_ptr<LogSerializerTask>(nullptr);
    // Interval used by log serialization task
    const std::chrono::microseconds serialization_interval_;

    // The log consumer task which flushes filled buffers to the disk
    std::shared_ptr<DiskLogConsumerTask> disk_log_writer_task_ = std::shared_ptr<DiskLogConsumerTask>(nullptr);
    // Interval used by disk consumer task
    const std::chrono::microseconds persist_interval_;
    // Threshold used by disk consumer task
    uint64_t persist_threshold_;

};

}  // namespace

