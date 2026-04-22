#include "cxl_memory.h"
#include <atomic>
#include <algorithm>
#include <cstring>
#include <immintrin.h>

CXLMemoryPool::CXLMemoryPool(void* hwcc_ptr, void* non_hwcc_ptr)
    : hwcc_(hwcc_ptr), non_hwcc_(non_hwcc_ptr) {
    // memory provided by caller; no allocation here
}

void CXLMemoryPool::sfence() {
    _mm_sfence();
}

void CXLMemoryPool::nt_store_u64(std::atomic<uint64_t>* addr, uint64_t value) {
    _mm_stream_si64(reinterpret_cast<long long*>(addr), static_cast<long long>(value));
}

void CXLMemoryPool::nt_store_u32(uint32_t* addr, uint32_t value) {
    _mm_stream_si32(reinterpret_cast<int*>(addr), static_cast<int>(value));
}

void CXLMemoryPool::nt_store_16(void* dst_aligned_16, const void* src_16) {
    __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src_16));
    _mm_stream_si128(reinterpret_cast<__m128i*>(dst_aligned_16), v);
}

void CXLMemoryPool::nt_memcpy(void* dst, const void* src, size_t len) {
    if (len == 0) {
        return;
    }

    auto* d = static_cast<unsigned char*>(dst);
    const auto* s = static_cast<const unsigned char*>(src);

#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
    auto nt_memcpy_tail_nt64 = [](unsigned char* dd, const unsigned char* ss, size_t ll) {
        size_t i = 0;
        for (; i + sizeof(uint64_t) <= ll; i += sizeof(uint64_t)) {
            uint64_t v;
            std::memcpy(&v, ss + i, sizeof(v));
            _mm_stream_si64(reinterpret_cast<long long*>(dd + i), static_cast<long long>(v));
        }
        if (i < ll) {
            uint64_t v = 0;
            std::memcpy(&v, ss + i, ll - i);
            _mm_stream_si64(reinterpret_cast<long long*>(dd + i), static_cast<long long>(v));
        }
    };

    auto nt_memcpy_avx512 = [&]() __attribute__((target("avx512f"))) {
        if (len < 512) {
            return;
        }

        while (len > 0 && (reinterpret_cast<uintptr_t>(d) & 63u)) {
            uint64_t v = 0;
            size_t n = std::min<size_t>(sizeof(uint64_t), len);
            std::memcpy(&v, s, n);
            _mm_stream_si64(reinterpret_cast<long long*>(d), static_cast<long long>(v));
            d += n;
            s += n;
            len -= n;
        }

        while (len >= 64) {
            __m512i v = _mm512_loadu_si512(reinterpret_cast<const void*>(s));
            _mm512_stream_si512(reinterpret_cast<__m512i*>(d), v);
            d += 64;
            s += 64;
            len -= 64;
        }
    };

    auto nt_memcpy_avx2 = [&]() __attribute__((target("avx2"))) {
        if (len < 256) {
            return;
        }

        while (len > 0 && (reinterpret_cast<uintptr_t>(d) & 31u)) {
            uint64_t v = 0;
            size_t n = std::min<size_t>(sizeof(uint64_t), len);
            std::memcpy(&v, s, n);
            _mm_stream_si64(reinterpret_cast<long long*>(d), static_cast<long long>(v));
            d += n;
            s += n;
            len -= n;
        }

        while (len >= 32) {
            __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(s));
            _mm256_stream_si256(reinterpret_cast<__m256i*>(d), v);
            d += 32;
            s += 32;
            len -= 32;
        }
    };

    nt_memcpy_avx512();

    nt_memcpy_tail_nt64(d, s, len);
    return;
#else
    size_t i = 0;
    for (; i + sizeof(uint64_t) <= len; i += sizeof(uint64_t)) {
        uint64_t v;
        std::memcpy(&v, s + i, sizeof(v));
        _mm_stream_si64(reinterpret_cast<long long*>(d + i), static_cast<long long>(v));
    }

    if (i < len) {
        uint64_t v = 0;
        std::memcpy(&v, s + i, len - i);
        _mm_stream_si64(reinterpret_cast<long long*>(d + i), static_cast<long long>(v));
    }
#endif
}

void CXLMemoryPool::clflush(const void* addr, size_t len) {
    constexpr size_t kCacheLineBytes = 64;
    uintptr_t start = reinterpret_cast<uintptr_t>(addr) & ~(static_cast<uintptr_t>(kCacheLineBytes - 1));
    uintptr_t end = reinterpret_cast<uintptr_t>(addr) + len;
    for (uintptr_t p = start; p < end; p += kCacheLineBytes) {
        _mm_clflush(reinterpret_cast<void*>(p));
    }
}
