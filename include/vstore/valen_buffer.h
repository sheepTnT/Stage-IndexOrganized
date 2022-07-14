//
// Created by zhangqian on 2022/1/24.
//

#ifndef MVSTORE_VALEN_BUFFER_H
#define MVSTORE_VALEN_BUFFER_H

#include "../common/macros.h"
#include "../common/object_pool.h"

namespace mvstore{

//static const uint32_t BUFFER_SEGMENT_SIZE = 1 << 14;
/**
* A RecordBufferSegment is a piece of (reusable) memory used to hold undo records. The segment internally keeps track
* of its memory usage.
*
* This class should not be used by itself. @see UndoBuffer, @see RedoBuffer
*
* Not thread-safe.
*/
class ValenBuffer{
public:
    ValenBuffer(){
        buffer_data_ = new char[BUFFER_SEGMENT_SIZE];
    }

    /**
     * @param size the amount of bytes to check for
     * @return Whether this segment have enough space left for size many bytes
     */
    bool HasBytesLeft(const uint32_t size) {
        auto cur_size = size_ + size;
        bool has_ = cur_size <= BUFFER_SEGMENT_SIZE;
        return  has_;
    }

    /**
     * Reserve space for a delta record of given size to be written in this segment. The segment must have
     * enough space for that record.
     *
     * @param size the amount of bytes to reserve
     * @return pointer to the head of the allocated record
     */
    char *Reserve(const uint32_t size) {
        latch.lock();
        PELOTON_ASSERT(HasBytesLeft(size), "reserve bytes record.");
        auto *result = buffer_data_ + size_;
        size_ += size;
        count_ = count_ + 1 ;
        latch.unlock();

        return result;
    }

    /**
     * Clears the buffer segment.
     *
     * @return self pointer for chaining
     */
    ValenBuffer *Reset() {
        latch.lock();
        size_ = 0;
        count_ = 0;
        latch.unlock();
        return this;
    }

    void erase(){
        latch.lock();
        count_ = count_ -1;
        latch.unlock();
    }

private:
    template <class RecordType>
    friend class IterableBuffer;

    char *buffer_data_;
    std::mutex latch;
    uint32_t size_ = 0;
    uint32_t count_ = 0;
};

/**
* Custom allocator used for the object pool of buffer segments
*/
class ValenBufferAllocator {
public:
    /**
     * Allocates a new BufferSegment
     * @return a new buffer segment
     */
    ValenBuffer *New() {
        auto *result = new ValenBuffer;
        PELOTON_ASSERT(reinterpret_cast<uintptr_t>(result) % 8 == 0 , "record buffer size % 8. ");
        return result;
    }

    /**
     * Resets the given buffer segment for use
     * @param reused the buffer to reuse
     */
    void Reuse(ValenBuffer *const reused) { reused->Reset(); }

    /**
     * Delete the given buffer segment and frees the memory
     * @param ptr the buffer to delete
     */
    void Delete(ValenBuffer *const ptr) { delete ptr; }
};

/**
* Type alias for an object pool handing out buffer segments
*/
using RecordBufferPool = ObjectPool<ValenBuffer, ValenBufferAllocator>;

/**
* holds the overwritten record versions,
 * 1. if the txn failure, it will be copied back to the tree,
 * 2. if the txn success commit, it will be released by the clear thread.
 */
class UndoBuffer {
public:

    /**
     * Constructs a new undo buffer, drawing its segments from the given buffer pool.
     * @param buffer_pool buffer pool to draw segments from
     */
    explicit UndoBuffer(RecordBufferPool *buffer_pool) : buffer_pool_(buffer_pool) {

    }

    /**
     * Destructs this buffer, releases all its segments back to the buffer pool it draws from.
     */
    ~UndoBuffer() {
        for (auto *segment : buffers_) buffer_pool_->Release(segment);
    }

    /**
     * @return true if UndoBuffer contains no UndoRecords, false otherwise
     */
    bool Empty() const { return buffers_.empty(); }

    /**
     * decrease the entrys count of the valen buffer
     * if the count =0, then the valen buffer can be release
     * @param index_
     */
    void Erase(uint32_t index_) const{
        ValenBuffer *val_buffer_ = buffers_[index_];
        //valenbuffer block count --
        val_buffer_->erase();
    }

    /**
     * Reserve an undo record with the given size.
     * @param size the size of the undo record to allocate
     * @return a new undo record with at least the given size reserved
     */
    std::pair<uint32_t, char *>NewEntry(const uint32_t size) {
        latch.Lock();
        if (buffers_.empty() || !buffers_.back()->HasBytesLeft(size)) {
            // we are out of space in the buffer. Get a new buffer segment.
            ValenBuffer *new_segment = nullptr;
            bool rel = false;
            while(!rel){
                new_segment = buffer_pool_->Get();
                if(new_segment != nullptr) {rel = true;}
            }
            PELOTON_ASSERT(reinterpret_cast<uintptr_t>(new_segment) % 8 == 0,
                           "a delta entry should be aligned to 8 bytes");
            buffers_.push_back(new_segment);
        }
        last_record_ = buffers_.back()->Reserve(size);
        latch.Unlock();

        return std::make_pair((buffers_.size()-1),last_record_);
    }

    /**
     * @return a pointer to the beginning of the last record requested,
     *          or nullptr if no record exists.
     */
    char *LastRecord() const { return last_record_; }

private:
    SpinLatch latch;
    RecordBufferPool *buffer_pool_;
    std::vector<ValenBuffer *> buffers_;
    char *last_record_ = nullptr;
};
class InnerNodeBuffer {
public:

    /**
     * Constructs a new undo buffer, drawing its segments from the given buffer pool.
     * @param buffer_pool buffer pool to draw segments from
     */
    explicit InnerNodeBuffer(RecordBufferPool *buffer_pool) : buffer_pool_(buffer_pool) {}

    /**
     * Destructs this buffer, releases all its segments back to the buffer pool it draws from.
     */
    ~InnerNodeBuffer() {
        for (auto *segment : buffers_) buffer_pool_->Release(segment);
    }

    /**
     * @return true if UndoBuffer contains no UndoRecords, false otherwise
     */
    bool Empty() const { return buffers_.empty(); }
    void Erase(uint32_t index_) const{
        ValenBuffer *val_buffer_ = buffers_[index_];
        val_buffer_->erase();
    }
    /**
     * Reserve an undo record with the given size.
     * @param size the size of the undo record to allocate
     * @return a new undo record with at least the given size reserved
     */
    char *NewEntry(const uint32_t size) {
        latch.Lock();
        if (buffers_.empty() || !buffers_.back()->HasBytesLeft(size)) {
            // we are out of space in the buffer. Get a new buffer segment.
            ValenBuffer *new_segment = nullptr;
            bool rel = false;
            while(!rel){
                new_segment = buffer_pool_->Get();
                if(new_segment != nullptr) {rel = true;}
            }
            PELOTON_ASSERT(reinterpret_cast<uintptr_t>(new_segment) % 8 == 0,
                           "a delta entry should be aligned to 8 bytes");
            buffers_.push_back(new_segment);
        }
        last_record_ = buffers_.back()->Reserve(size);
        latch.Unlock();
        return last_record_;
    }

    /**
     * @return a pointer to the beginning of the last record requested,
     *          or nullptr if no record exists.
     */
    char *LastRecord() const { return last_record_; }

private:
    SpinLatch latch;
    RecordBufferPool *buffer_pool_;
    std::vector<ValenBuffer *> buffers_;
    char *last_record_ = nullptr;
};

}
#endif
