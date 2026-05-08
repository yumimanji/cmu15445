#include <iostream>
#include <bitset>
#include <bit>
#include <limits>


inline auto ComputeBackZero(std::bitset<64> &bs) -> uint8_t
  {
    uint64_t num = bs.to_ullong();
    if (num == 0)
    {
      return 64;
    }
    return __builtin_ctzll(num);
  }

auto main() -> int
{
    // std::size_t x = 100;
    std::bitset<32> ret{0b000010110010};
    // std::cout << ret << "\n";
    std::cout << (ret >> 10) << std::endl;
    uint64_t val = ret.to_ullong();
    if (val == 0)
    {
        return 64;
    }
    constexpr int bits = std::numeric_limits<uint64_t>::digits;
    // std::cout << std::countl_zero(val)<< "\n";

    std::cout << (1 << 1) << "\n";
    // std::cout << sizeof(std::size_t) << "\n";
    auto tmp = std::bitset<64>(262144);
    std::cout << tmp << '\n';
    int x = ComputeBackZero(tmp);
    std::cout << x << '\n';
    
    return 0;
}