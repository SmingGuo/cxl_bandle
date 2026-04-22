#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>

class CXLMemoryPool {
public:
    static constexpr size_t HWCC_SIZE = 512 * 1024 * 1024ULL;           // metadata/descriptors
    static constexpr size_t NON_HWCC_SIZE = 30 * 1024 * 1024 * 1024ULL; // payloads

    CXLMemoryPool(void* hwcc_ptr, void* non_hwcc_ptr);
    
    void* get_hwcc() const { return hwcc_; }
    void* get_non_hwcc() const { return non_hwcc_; }
    size_t get_non_hwcc_size() const { return NON_HWCC_SIZE; }
    
    void sfence();
    void nt_store_u64(std::atomic<uint64_t>* addr, uint64_t value);

    // Non-temporal stores for writing to non-coherent / write-combining memory.
    void nt_store_u32(uint32_t* addr, uint32_t value);
    // 16B non-temporal store (dst must be 16B-aligned).
    void nt_store_16(void* dst_aligned_16, const void* src_16);
    void nt_memcpy(void* dst, const void* src, size_t len);

    // Flush CPU cachelines covering [addr, addr+len) back to memory.
    // Use together with sfence() to ensure flush completion when needed.
    void clflush(const void* addr, size_t len);
    
private:
    void* hwcc_;
    void* non_hwcc_;
};
