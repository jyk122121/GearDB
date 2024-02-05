
#include "generator.h"

namespace leveldb {
namespace util {
uint64_t fnvhash64(uint64_t val) {
  //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
  uint64_t hashval = kFNVOffsetBasis64;

  for (int i = 0; i < 8; i++) {
    uint64_t octet = val & 0x00ff;
    val = val >> 8;

    hashval = hashval ^ octet;
    hashval = hashval * kFNVPrime64;
  }
  return hashval;
}
} // namespace util
} // namespace rocksdb