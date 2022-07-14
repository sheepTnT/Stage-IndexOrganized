#include <queue>
#include <utility>
#include <thread>  // NOLINT
#include "../include/common/spin_latch.h"
#include "../include/common/bitmap.h"
#include "../include/common/dedicated_thread_registry.h"
#include "../include/vstore/version_store.h"
#include "../include/vstore/logger.h"

namespace mvstore {

void DiskLogConsumerTask::RunTask() {
    run_task_ = true;
    DiskLogConsumerTaskLoop();
}

void DiskLogConsumerTask::Terminate() {
    // If the task hasn't run yet, yield the thread until it's started
    while (!run_task_) std::this_thread::yield();
    PELOTON_ASSERT(run_task_,"disk log consumer is terminating !");
    // Signal to terminate and force a flush so task persists before LogManager closes buffers
    run_task_ = false;
    disk_log_writer_thread_cv_.notify_one();
}

void DiskLogConsumerTask::WriteBuffersToLogFile() {
    // Persist all the filled buffers to the disk
//    SerializedLogs logs;
    BufferedLogWriter *logs;
    while (!filled_buffer_queue_->Empty()) {
        // Dequeue filled buffers and flush them to disk, as well as storing commit callbacks
        filled_buffer_queue_->Dequeue(&logs);
        if (logs != nullptr) {
            // Need the nullptr check because read-only txns don't serialize any buffers, but generate callbacks to be invoked
            current_data_written_ += logs->FlushBuffer();
        }
//        commit_callbacks_.insert(commit_callbacks_.end(), logs.second.begin(),
//                                 logs.second.end());
        // Enqueue the flushed buffer to the empty buffer queue
        if (logs != nullptr) {
            // nullptr check for the same reason as above
            empty_buffer_queue_->Enqueue(logs);
        }
    }
}

uint64_t DiskLogConsumerTask::PersistLogFile() {
    // buffers_ may be empty but we have callbacks to invoke due to read-only txns
    if (!buffers_->empty()) {
        // Force the buffers to be written to disk. Because all buffers log to the same file, it suffices to call persist on any buffer.
        buffers_->front().Persist();
    }
//    const auto num_buffers = commit_callbacks_.size();
    // Execute the callbacks for the transactions that have been persisted
//    for (auto &callback : commit_callbacks_) callback.first(callback.second);
//    commit_callbacks_.clear();
//    return num_buffers;
      return 0;
}

void DiskLogConsumerTask::DiskLogConsumerTaskLoop() {
    // input for this operating unit
    uint64_t num_bytes = 0, num_buffers = 0;

    // Keeps track of how much data we've written to the log file since the last persist
    current_data_written_ = 0;
    // Initialize sleep period
    auto curr_sleep = persist_interval_;
    auto next_sleep = curr_sleep;
    const std::chrono::microseconds max_sleep = std::chrono::microseconds(10000);
    // Time since last log file persist
    auto last_persist = std::chrono::high_resolution_clock::now();
    // Disk log consumer task thread spins in this loop. When notified or periodically, we wake up and process serialized buffers
    do {
//    const bool logging_metrics_enabled =
//        common::thread_context.metrics_store_ != nullptr &&
//        common::thread_context.metrics_store_->ComponentToRecord(
//            metrics::MetricsComponent::LOGGING);
//
//    if (logging_metrics_enabled &&
//        !common::thread_context.resource_tracker_.IsRunning()) {
//      // start the operating unit resource tracker
//      common::thread_context.resource_tracker_.Start();
//    }

        curr_sleep = next_sleep;
        {
            // Wait until we are told to flush buffers
            std::unique_lock<std::mutex> lock(persist_lock_);
            // Wake up the task thread if:
            // 1) The serializer thread has signalled to persist all non-empty buffers to disk 2) There is a filled buffer to write to the disk 3) LogManager has shut down the task 4) Our persist interval timed out

            bool signaled =
                    disk_log_writer_thread_cv_.wait_for(lock, curr_sleep, [&] {
//                        return force_flush_ || !filled_buffer_queue_->Empty() || !run_task_;
                        return force_flush_ || !run_task_;
                    });
            next_sleep = signaled ? persist_interval_ : curr_sleep * 2;
            next_sleep = std::min(next_sleep, max_sleep);
        }

        // Flush all the buffers to the log file
        WriteBuffersToLogFile();

        // We persist the log file if the following conditions are met
        // 1) The persist interval amount of time has passed since the last persist
        // 2) We have written more data since the last persist than the threshold
        // 3) We are signaled to persist
        // 4) We are shutting down this task
        bool timeout = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() -
                last_persist) > curr_sleep;

        if (timeout || current_data_written_ > persist_threshold_ || force_flush_ ||
            !run_task_) {
            std::unique_lock<std::mutex> lock(persist_lock_);
            num_buffers = PersistLogFile();
            num_bytes = current_data_written_;
            // Reset meta data
            last_persist = std::chrono::high_resolution_clock::now();
            current_data_written_ = 0;
            force_flush_ = false;

            // Signal anyone who forced a persist that the persist has finished
            persist_cv_.notify_all();
        }

//    if (logging_metrics_enabled && num_buffers > 0) {
//      // Stop the resource tracker for this operating unit
//      common::thread_context.resource_tracker_.Stop();
//      auto &resource_metrics =
//          common::thread_context.resource_tracker_.GetMetrics();
//      common::thread_context.metrics_store_->RecordConsumerData(
//          num_bytes, num_buffers, persist_interval_.count(), resource_metrics);
        num_bytes = num_buffers = 0;
//    }
    } while (run_task_);
    // Be extra sure we processed everything
    WriteBuffersToLogFile();
    PersistLogFile();
}
/**
 * LogSerializerTaskLoop()->Process()
 */
void LogSerializerTask::LogSerializerTaskLoop() {
    auto curr_sleep = serialization_interval_;
    // TODO(Gus): Make max back-off a settings manager setting
    const std::chrono::microseconds max_sleep =
            std::chrono::microseconds(10000);  // We cap the back-off in case of long gaps with no transactions

    uint64_t num_bytes = 0, num_records = 0, num_txns = 0;

    do {
//    const bool logging_metrics_enabled =
//        common::thread_context.metrics_store_ != nullptr &&
//        common::thread_context.metrics_store_->ComponentToRecord(metrics::MetricsComponent::LOGGING);
//
//    if (logging_metrics_enabled && !common::thread_context.resource_tracker_.IsRunning()) {
//      // start the operating unit resource tracker
//      common::thread_context.resource_tracker_.Start();
//    }

        // Serializing is now on the "critical txn path" because txns wait to commit until their logs are serialized. Thus,
        // a sleep is not fast enough. We perform exponential back-off, doubling the sleep duration if we don't process any
        // buffers in our call to Process. Calls to Process will process as long as new buffers are available. We only
        // sleep as part of this exponential backoff when there are logs that need to be processed and we wake up when there
        // are new logs to be processed.
        if (empty_) {
            std::unique_lock<std::mutex> guard(flush_queue_latch_);
            sleeping_ = true;
            flush_queue_cv_.wait_for(guard, curr_sleep);
            sleeping_ = false;
        }

        // If Process did not find any new buffers, we perform exponential back-off to reduce our rate of polling for new
        // buffers. We cap the maximum back-off, since in the case of large gaps of no txns, we don't want to unboundedly
        // sleep
        std::tie(num_bytes, num_records, num_txns) = Process();
        curr_sleep = std::min(num_records > 0 ? serialization_interval_ : curr_sleep * 2, max_sleep);

//    if (logging_metrics_enabled && num_records > 0) {
//      // Stop the resource tracker for this operating unit
//      common::thread_context.resource_tracker_.Stop();
//      auto &resource_metrics = common::thread_context.resource_tracker_.GetMetrics();
//      common::thread_context.metrics_store_->RecordSerializerData(num_bytes, num_records, num_txns,
//                                                                  serialization_interval_.count(), resource_metrics);
//      num_bytes = num_records = num_txns = 0;
//    }
    } while (run_task_);
    // To be extra sure we processed everything
    Process();
    PELOTON_ASSERT(flush_queue_.empty(),"flush queue is not empty.");
}
/**
 * process the log record, serialize those records to writer buffers
 * Process()->SerializeBuffer()->SerializeRecord()->WriteValue()
 * Process()->HandFilledBufferToWriter()
 * @return
 */
std::tuple<uint64_t, uint64_t, uint64_t> LogSerializerTask::Process() {
    uint64_t num_bytes = 0, num_records = 0, num_txns = 0;

    bool buffers_processed = false;

    {
        SpinLatch::ScopedSpinLatch serialization_guard(&serialization_latch_);
//        PELOTON_ASSERT(serialized_txns_.empty());
        // We continually grab all the buffers until we find there are no new buffers. This way we serialize buffers that
        // came in during the previous serialization loop

        // Continually loop, break out if there's no new buffers
        while (true) {
            // In a short critical section, get all buffers to serialize. We move them to a temp queue to reduce contention on
            // the queue transactions interact with
            {
                std::unique_lock<std::mutex> guard(flush_queue_latch_);

                // There are no new buffers, so we can break
                if (flush_queue_.empty()) {
                    break;
                }

                temp_flush_queue_ = std::move(flush_queue_);
                flush_queue_ = std::queue<ValenBuffer *>();
                empty_ = true;
            }

            // Loop over all the new buffers we found
            while (!temp_flush_queue_.empty()) {
                ValenBuffer *buffer = temp_flush_queue_.front();
                temp_flush_queue_.pop();

                // Serialize the Redo buffer and release it to the buffer pool
                IterableBuffer<LogRecord> task_buffer(buffer);
                const auto num_bytes_records_and_txns = SerializeBuffer(&task_buffer);
                buffer_pool_->Release(buffer);
                num_bytes += std::get<0>(num_bytes_records_and_txns);
                num_records += std::get<1>(num_bytes_records_and_txns);
                num_txns += std::get<2>(num_bytes_records_and_txns);
            }

            buffers_processed = true;
        }

        // Mark the last buffer that was written to as full
        if (buffers_processed) HandFilledBufferToWriter();

        // Bulk remove all the transactions we serialized. This prevents having to take the TimestampManager's latch once
        // for each timestamp we remove.
//        for (const auto &txns : serialized_txns_) {
//      txns.first->RemoveTransactions(txns.second);
            // after serialized, this txn can be gc processing
//            txns->SetTxnStates(TxnState::SERIALIZED);
//        }
//        serialized_txns_.clear();
    }

    std::tuple<uint64_t, uint64_t, uint64_t> tuple_(num_bytes, num_records, num_txns);
    return tuple_;
}

/**
* Used by the serializer thread to get a buffer to serialize data to
* @return buffer to write to
*/
BufferedLogWriter *LogSerializerTask::GetCurrentWriteBuffer() {
    if (filled_buffer_ == nullptr) {
        empty_buffer_queue_->Dequeue(&filled_buffer_);
    }
    return filled_buffer_;
}

/**
* Hand over the current buffer and commit callbacks for commit records in
 * that buffer to the log consumer task
*/
void LogSerializerTask::HandFilledBufferToWriter() {
    // Hand over the filled buffer
    filled_buffer_queue_->Enqueue(filled_buffer_);
    // Signal disk log consumer task thread that a buffer has been handed over
    disk_log_writer_thread_cv_->notify_one();
    // Mark that the task doesn't have a buffer in its possession to which it can write to
//    commits_in_buffer_.clear();
    filled_buffer_ = nullptr;
}

std::tuple<uint64_t, uint64_t, uint64_t> LogSerializerTask::SerializeBuffer(
        IterableBuffer<LogRecord> *buffer_to_serialize) {
    uint64_t num_bytes = 0, num_records = 0, num_txns = 0;

    // Iterate over all redo records in the redo buffer through the provided iterator
    for (LogRecord &record : *buffer_to_serialize) {
        switch (record.GetType()) {
            case (LogRecordType::COMMIT_TXN): {
//                auto *commit_record = record.GetUnderlyingRecordBodyAs<CommitRecord>();

                // If a transaction is read-only, then the only record
                // it generates is its commit record. This commit record is
                // necessary for the transaction's callback function to be invoked,
                // but there is no need to serialize it, as
                // it corresponds to a transaction with nothing to redo.
                num_bytes += SerializeRecord(record);
//                commits_in_buffer_.emplace_back(commit_record->CommitCallback(), commit_record->CommitCallbackArg());
                // Once serialization is done, we notify the txn manager to
                // let GC know this txn is ready to clean up
//                serialized_txns_.push_back(commit_record->GetTxn());
                num_txns++;
                break;
            }

          case (LogRecordType::ABORT_TXN): {
            // If an abort record shows up at all, the transaction cannot be read-only
            num_bytes += SerializeRecord(record);
//            auto *abord_record = record.GetUnderlyingRecordBodyAs<AbortRecord>();
//            serialized_txns_[abord_record->TimestampManager()].push_back(record.TxnBegin());
            num_txns++;
            break;
          }

            default:
                // Any record that is not a commit record is always serialized.`
                num_bytes += SerializeRecord(record);
        }
        num_records++;

        //TODO: free the log record memory space, votial and non-votial
    }
    std::tuple<uint64_t, uint64_t, uint64_t> tuple_(num_bytes, num_records, num_txns);
    return tuple_;
}

uint64_t LogSerializerTask::SerializeRecord(const LogRecord &record) {
    uint64_t num_bytes = 0;
    // First, serialize out fields common across all LogRecordType's.

    // Note: This is the in-memory size of the log record itself,
    // i.e. inclusive of padding and not considering the size
    //   of any potential varlen entries.
    // It is logically different from the size of the serialized record,
    //   which the log manager generates in this function.
    // In particular, the later value is very likely to be strictly smaller when the
    //   LogRecordType is REDO.
    // On recovery, the goal is to turn the serialized format back
    //   into an in-memory log record of this size.
    num_bytes += WriteValue(record.Size());
    num_bytes += WriteValue(record.GetType());
    //begin id
    num_bytes += WriteValue(record.GetBeginID());
    num_bytes += WriteValue(record.GetCommitID());
    // header----------------------------------------------------------------------------------
    // | redo record size (32bit) | record type (8bit) | txn_begin_id (32bits) | txn_end_id(32bits)
    // body----------------------------------------------------------------------------------
    // | database_oid (32-bit) | table_oid (32-bit) | key_length (32-bit)| key_value(bytes) |
    // ----------------------------------------------------------------------------------
    // | num_cols (16-bit) |  col_id (16-bit) | col1_length (32-bit) | col1_value (bytes) |
    // ----------------------------------------------------------------------------------
    // ----------------------------------------------------------------------------------
    switch (record.GetType()) {
        case LogRecordType::UPDATE: {
//            auto record_body = record;
            const Catalog *catalog_ = record.GetTupleCatalog();
            auto num_cols = catalog_->field_cnt;
//            num_bytes += WriteValue(record_body->GetDatabaseOid());
            num_bytes += WriteValue(catalog_->table_name);

            //get the key bitmap
            uint32_t key_total_length = catalog_->key_total_length;
            uint8_t *key_columns = catalog_->key_bitmap;
            uint32_t key_bitmap_num_bytes = RawBitmap::SizeInBytes(num_cols);
            auto *key_bitmap_buffer = new uint8_t[key_bitmap_num_bytes];
            memcpy(key_bitmap_buffer, key_columns, key_bitmap_num_bytes);
            auto *key_bitmap = reinterpret_cast<RawBitmap *>(key_bitmap_buffer);

            num_bytes += WriteValue(key_total_length);

            char *tuple_slot = record.GetNextTupleSlot();
            BaseTuple tuple_(tuple_slot);
            for(uint8_t c_l = 0 ; c_l < num_cols; c_l++){
                //0 means that c_l is not in key columns set
                if(!key_bitmap->Test(c_l)){
                    continue;
                }
                //get key value from the version tuple,
                //since we usually do not update the key values.
                char *k_l_v = tuple_.GetValue(key_columns[c_l]);

                auto key_length = catalog_->get_field_size(key_columns[c_l]);
                //write out the key_value serializely
                num_bytes += WriteValue(k_l_v, key_length);
            }

            //get the delta bitmap
            uint8_t *delta_bit_map = record.GetBits();
            uint32_t bitmap_num_bytes = RawBitmap::SizeInBytes(num_cols);
            auto *bitmap_buffer = new uint8_t[bitmap_num_bytes];
            memcpy(bitmap_buffer, delta_bit_map, bitmap_num_bytes);
            auto *bitmap = reinterpret_cast<RawBitmap *>(bitmap_buffer);

            //column numbers
            num_bytes += WriteValue(num_cols);

            auto *delta = record.GetDeltaData();
            uint32_t leg = 0;
            // Write out attribute values
            for (uint32_t i = 0; i < num_cols; i++) {
                //0 means not update in our definition of delta
                //then, there is nothing to serialize out.
                if(!bitmap->Test(i)){
                  continue;
                }
                //column id, length
                num_bytes += WriteValue(i);
                uint32_t col_size = catalog_->get_field_size(i);
                num_bytes += WriteValue(col_size);

                //column id value
                // Get the column id of the current column .
                num_bytes += WriteValue((delta+leg) , col_size);
                leg = leg + col_size;
            }

            COMPILER_MEMORY_FENCE;

            PELOTON_ASSERT(leg == record.GetLength(),"log update, delta length != leg. ");

            delete [] key_bitmap_buffer;
            delete [] bitmap_buffer;

            break;
        }
        case LogRecordType::INSERT: {
//            auto record_body = record;
            const Catalog *catalog_ = record.GetTupleCatalog();
            auto num_cols = catalog_->field_cnt;
//            num_bytes += WriteValue(record_body->GetDatabaseOid());
            num_bytes += WriteValue(catalog_->table_name);

            uint32_t key_total_length = catalog_->key_total_length;
            uint8_t *key_columns = catalog_->key_bitmap;
            uint32_t key_bitmap_num_bytes = RawBitmap::SizeInBytes(num_cols);
            auto *key_bitmap_buffer = new uint8_t[key_bitmap_num_bytes];
            memcpy(key_bitmap_buffer, key_columns, key_bitmap_num_bytes);
            auto *key_bitmap = reinterpret_cast<RawBitmap *>(key_bitmap_buffer);

            char *key_value = new char[key_total_length];
            memset(key_value, 0 , key_total_length);
            //key length, key value
            num_bytes += WriteValue(key_total_length);
            num_bytes += WriteValue(key_value, key_total_length);

            //column numbers
            num_bytes += WriteValue(num_cols);

            auto *delta = record.GetDeltaData();
            uint32_t leg = 0;
            // Write out attribute values
            for (uint32_t i = 0; i < num_cols; i++) {
                //column id, length
                num_bytes += WriteValue(i);
                uint32_t col_size = catalog_->get_field_size(i);
                num_bytes += WriteValue(col_size);

                //column id value
                // Get the column id of the current column .
                num_bytes += WriteValue((delta+leg) , col_size);

                //0 means that c_l is not in key columns set
                if(key_bitmap->Test(i)){
                    memcpy(key_value,delta+leg, col_size);
                }

                leg = leg + col_size;
            }

            COMPILER_MEMORY_FENCE;

            PELOTON_ASSERT(leg == record.GetLength(),"log insert, delta length != leg. ");

            delete [] key_bitmap_buffer;
            delete [] key_value;

        }
        case LogRecordType::DELETE: {
//            auto record_body = record;
            const Catalog *catalog_ = record.GetTupleCatalog();
            auto num_cols = catalog_->field_cnt;
//            num_bytes += WriteValue(record_body->GetDatabaseOid());
            num_bytes += WriteValue(catalog_->table_name);

            //get the key bitmap
            uint32_t key_total_length = catalog_->key_total_length;
            uint8_t *key_columns = catalog_->key_bitmap;
            uint32_t key_bitmap_num_bytes = RawBitmap::SizeInBytes(num_cols);
            auto *key_bitmap_buffer = new uint8_t[key_bitmap_num_bytes];
            memcpy(key_bitmap_buffer, key_columns, key_bitmap_num_bytes);
            auto *key_bitmap = reinterpret_cast<RawBitmap *>(key_bitmap_buffer);

            //write out the key length
            num_bytes += WriteValue(key_total_length);

            char *tuple_slot = record.GetNextTupleSlot();
            BaseTuple tuple_(tuple_slot);
            for(uint8_t c_l = 0 ; c_l < num_cols; c_l++){
                //0 means that c_l is not in key columns set
                if(!key_bitmap->Test(c_l)){
                    continue;
                }
                //get key value from the version tuple,
                //since we usually do not update the key values.
                char *k_l_v = tuple_.GetValue(key_columns[c_l]);

                auto key_length = catalog_->get_field_size(key_columns[c_l]);
                //write out the key_value serializely
                num_bytes += WriteValue(k_l_v, key_length);
            }

            delete [] key_bitmap_buffer;

            break;
        }
        case LogRecordType::BEGIN_TXN :
        case LogRecordType::COMMIT_TXN :
        case LogRecordType::ABORT_TXN: {
          // AbortRecord does not hold any additional metadata
          break;
        }
        default: {
            // Type not Identified
            LOG_ERROR("unimplemented  record type '%u'", record.GetType());
            break;
        }

    }

    return num_bytes;
}

uint32_t LogSerializerTask::WriteValue(const void *val, const uint32_t size) {
    // Serialize the value and copy it to the buffer
    BufferedLogWriter *out = GetCurrentWriteBuffer();
    uint32_t size_written = 0;

    while (size_written < size) {
        const char *val_byte = reinterpret_cast<const char *>(val) + size_written;
        size_written += out->BufferWrite(val_byte, size - size_written);
        if (out->IsBufferFull()) {
            // Mark the buffer full for the disk log consumer task thread to flush it
            HandFilledBufferToWriter();
            // Get an empty buffer for writing this value
            out = GetCurrentWriteBuffer();
        }
    }
    return size;
}


void WblLogManager::Start() {
    PELOTON_ASSERT(!run_log_manager_, "wbl log is already running");
    // Initialize buffers for logging
    for (size_t i = 0; i < num_buffers_; i++) {
        buffers_.emplace_back(BufferedLogWriter(log_file_path_.c_str()));
    }
    for (size_t i = 0; i < num_buffers_; i++) {
        empty_buffer_queue_.Enqueue(&buffers_[i]);
    }

    run_log_manager_ = true;

    // Register DiskLogConsumerTask
    disk_log_writer_task_ = std::make_shared<DiskLogConsumerTask>(
            persist_interval_, persist_threshold_,
            &buffers_, &empty_buffer_queue_,
            &filled_buffer_queue_);
    DedicatedThreadRegistry::GetInstance().
                        RegisterDedicatedThread<DiskLogConsumerTask>(this, disk_log_writer_task_);

    // Register LogSerializerTask
    log_serializer_task_ = std::make_shared<LogSerializerTask>(
            serialization_interval_, buffer_pool_,
            &empty_buffer_queue_,
            &filled_buffer_queue_,
            &disk_log_writer_task_->disk_log_writer_thread_cv_);
    DedicatedThreadRegistry::GetInstance().
                        RegisterDedicatedThread<LogSerializerTask>(this, log_serializer_task_);

}

//void WalLogManager::ForceFlush() {
//  // Force the serializer task to serialize buffers
//  log_serializer_task_->Process();
//  // Signal the disk log consumer task thread to persist the buffers to disk
//  std::unique_lock<std::mutex> lock(disk_log_writer_task_->persist_lock_);
//  disk_log_writer_task_->force_flush_ = true;
//  disk_log_writer_task_->disk_log_writer_thread_cv_.notify_one();
//
//  // Wait for the disk log consumer task thread to persist the logs
//  disk_log_writer_task_->persist_cv_.wait(
//      lock, [&] { return !disk_log_writer_task_->force_flush_; });
//}

void WblLogManager::PersistAndStop() {
    PELOTON_ASSERT(run_log_manager_, "wbl log is not running!");
    run_log_manager_ = false;

    // Signal all tasks to stop. The shutdown of the tasks will trigger any remaining logs to be serialized, writen to the log file, and persisted. The order in which we shut down the tasks is important, we must first serialize, then shutdown the disk consumer task (reverse order of Start())
    auto result = DedicatedThreadRegistry::GetInstance().StopTask(this,log_serializer_task_);

    PELOTON_ASSERT(result, "dedicated thread stop the task log serializer.");

    result = DedicatedThreadRegistry::GetInstance().StopTask(this,disk_log_writer_task_);

    PELOTON_ASSERT(result, "dedicated thread stop the task disk log writer.");
//    PELOTON_ASSERT(filled_buffer_queue_.Empty());

    // Close the buffers corresponding to the log file
    for (auto buf : buffers_) {
        buf.Close();
    }
    // Clear buffer queues
    empty_buffer_queue_.Clear();
//    filled_buffer_queue_.Clear();
    buffers_.clear();
}

void WblLogManager::AddBufferToFlushQueue(ValenBuffer *const buffer_segment) {
    PELOTON_ASSERT(run_log_manager_, "log manager is not running.");
    //when the memory allocated reach the
    //threshold of the setting, physical memory 60%,
    //then flush the log record and recycle the space
    //TODO: add to flush queue buffer
    log_serializer_task_->AddBufferToFlushQueue(buffer_segment);
}


}