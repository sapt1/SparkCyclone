#ifndef _VEC_OPERATIONS_ 
#define _VEC_OPERATIONS_

#include <limits>
#include <cmath>
#include "utility.cc"
#include "utility.hpp"
#include "set_operations.hpp"
#include "conditions_for_find.hpp"
#include "../text/find_condition.hpp"

#define OP_VLEN 1024
#define NOVEC_LEN 20

/*
 *  This header contains frequently used vector operations in ML algorithms
 *  similar to following numpy operations on 1D array
 *    numpy.sum(x) -> vector_sum(x)
 *    numpy.square(x) -> vector_square(x)
 *    numpy.sqrt(x) -> vector_sqrt(x) for non-integral x 
 *                  -> vector_sqrt_inplace(x) to perform sqrt inplace for non-integral x (returns void)
 *                  -> integral_vector_sqrt(x) for integral x (returns vector<double>)
 *    numpy.mean(x) -> vector_mean(x)
 *    numpy.sort(x) -> vector_sort(x)
 *    numpy.count_nonzero(x) -> vector_count_nonzero(x)
 *      additionally available: vector_count_positives(x) and vector_count_negatives(x)
 *      also available: vector_count_equal(x, k) -> countys no. of elements in x is equal to k
 *                      vector_is_uniform(x) -> if all elements in x are of same value
 *    numpy.zeros(sz) -> vector_zeros(sz)
 *    numpy.ones(sz) -> vector_ones(sz)
 *    numpy.full(sz, val) [numpy.ndarray.fill(val)] -> vector_full(sz, val)
 *    numpy.ndarray.astype(dtype) -> vector_astype<T>()
 *    numpy.arange(st, end, step) -> vector_arrange(st, end, step)
 *    numpy.unique(x, ...) -> vector_unique(x, ...)
 *    numpy.bincount(x) -> vector_bincount(x) (x should be non-negative int-vector)
 *    numpy.divide(x, y) -> vector_divide(x, y) or x / y
 *    numpy.multiply(x, y) -> vector_multiply(x, y) or x * y
 *    numpy.add(x, y) -> vector_add(x, y) or x + y
 *    numpy.subtract(x, y) -> vector_subtract(x, y) or x - y
 *    numpy.negative(x) -> vector_negative(x) or -x
 *    numpy.dot(x, y) or blas.dot(x, y) -> vector_dot(x, y) 
 *    numpy.dot(x, x) or numpy.sum(numpy_square(x)) -> vector_squared_sum(x)
 *    numpy.linalg.norm(x) -> vector_norm(x) [for euclidean norm of vector, x]
 *    numpy.sum(numpy_square(x - y)) -> vector_ssd(x, y) [sum squared difference]
 *    numpy.sum(numpy_square(x - numpy.mean(x)) -> vector_ssmd(x) [sum squared mean difference]
 *    numpy.sum(x * scalar) -> vector_scaled_sum(x, scala)
 *    numpy.sum(x / scalar) -> vector_scaled_sum(x, 1 / scala)
 *    blas.axpy(x, y, alpha) -> vector_axpy(x, y, alpha) [returns alpga * x + y]
 *    numpy.log(x) -> vector_log(x)
 *    numpy.negative(numpy.log(x)) or -numpy.log(x) -> vector_negative_log(x) 
 *    numpy.argmax(x) -> vector_argmax(x)
 *    numpy.argmin(x) -> vector_argmin(x)
 *    numpy.amax(x) -> vector_amax(x)
 *    numpy.amin(x) -> vector_amin(x)
 *    numpy.clip(x, min, max) -> vector_clip(x, min, max)
 *    numpy.take(x, idx) -> vector_take(x, idx)
 *    sklearn.preprocessing.binarize(x, thr) -> vector_binarize(x, thr)
 *    scipy.misc.logsumexp(x) -> vector_logsumexp(x)
 *    numpy.exp(x) -> vector_exp(x) [to perform exp() on non-integral vector x]
 *                 -> vector_exp_inplace(x) [to perform exp() in-place on non-integral vector x]
 *
 *  Additionally contains:
 *    debug_print_vector(x, n) - to print fist n and last n elements in vector x
 *    do_allgather(x) - returns gathered vector from all process (must be called by all process from worker side)
 *    count operations for: zero, nonzero, positive, negative, finite, boundary etc.
 *    find index operations for: zero, nonzero, one, positive, negative, Tmax, Tmin, gt, ge, lt, le, eq, neq etc.
 *    	e.g., vector_find_zero(vec)  -> returns index having zeros in vec
 *    	      vector_find_ge(vec, 5) -> returns index having values >= 5
 *    make_key_value_pair(key, val): returns vector<key, val>
 *    vector_min_pair(x, y): reduction by min for two vector of pairs<T,I>, returns pair vector containing minimums
 *    vector_min_index(x, y): reduction by min for two vector of pairs<T,I>, returns vector of min indices
 *    vector_min_value(x, y): reduction by min for two vector of pairs<T,I>, returns vector of min values
 *    vector_max_pair(x, y): reduction by max for two vector of pairs<T,I>, returns pair vector containing maximums
 *    vector_max_index(x, y): reduction by max for two vector of pairs<T,I>, returns vector of max indices
 *    vector_max_value(x, y): reduction by max for two vector of pairs<T,I>, returns vector of max values
 *    vector_right_shift(x, tid): right-shift by 1 position for all elements from 0th index to 'tid-1'th index;
 *                                place value in 'tid'th index at 0.
 *    vector_right_shift_inplace(x, tid): inplace version of the above to shift elements in input 'x' itself.
 *
 */

namespace frovedis {

// regarding find functions - uses loop-raked find_condition() defined in text module
template <class T, class F>
std::vector<size_t>
vector_find_condition(const std::vector<T>& vec, 
                      const F& cond) {
  return find_condition(vec.data(), vec.size(), cond); // defined in text module
}

template <class T>
std::vector<size_t>
vector_find_nonzero(const std::vector<T>& vec) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_nonzero<T>()); 
}

template <class T>
std::vector<size_t>
vector_find_zero(const std::vector<T>& vec) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_zero<T>()); 
}

template <class T>
std::vector<size_t>
vector_find_one(const std::vector<T>& vec) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_one<T>()); 
}

template <class T>
std::vector<size_t>
vector_find_positive(const std::vector<T>& vec) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_positive<T>()); 
}

template <class T>
std::vector<size_t>
vector_find_negative(const std::vector<T>& vec) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_negative<T>()); 
}

template <class T>
std::vector<size_t>
vector_find_tmax(const std::vector<T>& vec) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_tmax<T>()); 
}

template <class T>
std::vector<size_t>
vector_find_not_tmax(const std::vector<T>& vec) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_not_tmax<T>()); 
}

template <class T>
std::vector<size_t>
vector_find_tmin(const std::vector<T>& vec) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_tmin<T>()); 
}

template <class T>
std::vector<size_t>
vector_find_not_tmin(const std::vector<T>& vec) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_not_tmin<T>()); 
}

template <class T>
std::vector<size_t>
vector_find_binary(const std::vector<T>& vec) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_binary<T>()); 
}

template <class T>
std::vector<size_t>
vector_find_ge(const std::vector<T>& vec, const T& threshold) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_ge<T>(threshold));
}

template <class T>
std::vector<size_t>
vector_find_ge_and_neq(const std::vector<T>& vec, 
                       const T& threshold,
                       const T& threshold2) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_ge_and_neq<T>(threshold, 
                                                          threshold2));
}

template <class T>
std::vector<size_t>
vector_find_gt(const std::vector<T>& vec, const T& threshold) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_gt<T>(threshold));
}

template <class T>
std::vector<size_t>
vector_find_gt_and_neq(const std::vector<T>& vec, 
                       const T& threshold,
                       const T& threshold2) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_gt_and_neq<T>(threshold,
                                                          threshold2));
}

template <class T>
std::vector<size_t>
vector_find_le(const std::vector<T>& vec, const T& threshold) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_le<T>(threshold));
}

template <class T>
std::vector<size_t>
vector_find_le_and_neq(const std::vector<T>& vec, 
                       const T& threshold,
                       const T& threshold2) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_le_and_neq<T>(threshold, 
                                                          threshold2));
}

template <class T>
std::vector<size_t>
vector_find_lt(const std::vector<T>& vec, const T& threshold) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_lt<T>(threshold));
}

template <class T>
std::vector<size_t>
vector_find_lt_and_neq(const std::vector<T>& vec, 
                       const T& threshold,
                       const T& threshold2) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_lt_and_neq<T>(threshold,
                                                          threshold2));
}

template <class T>
std::vector<size_t>
vector_find_eq(const std::vector<T>& vec, const T& threshold) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_eq<T>(threshold));
}

template <class T>
std::vector<size_t>
vector_find_neq(const std::vector<T>& vec, const T& threshold) {
  if (vec.size() == 0) return std::vector<size_t>();
  else return vector_find_condition(vec, is_neq<T>(threshold));
}

// * if limit = 0, it prints all elements in the input vector.
// * if limit = x and size of vector is more than twice of x, 
// then it prints first "x" and last "x" elements in the input vector.
// * if size of vector is less than twice of x, then it prints all elements.
template <class T>
void debug_print_vector(const std::vector<T>& vec,
                        size_t limit = 0) {
  if (limit == 0 || vec.size() < 2*limit) {
    for(auto& i: vec){ std::cout << i << " "; }
    std::cout << std::endl;
  }
  else {
    for(size_t i = 0; i < limit; ++i) std::cout << vec[i] << " ";
    std::cout << " ... ";
    auto size = vec.size();
    for(size_t i = size - limit; i < size; ++i) std::cout << vec[i] << " ";
    std::cout << std::endl;
  }
}

// show() for debugging with tagged (named) vector...
template <class T>
void show(const std::string& msg,
          const std::vector<T>& vec,
          const int& limit = 10) {
  std::cout << msg; debug_print_vector(vec, limit);
}

// must be called from local process (worker)
template <class T>
std::vector<T> do_allgather(std::vector<T>& vec) {
  int size = vec.size();
  auto nproc = get_nodesize();
  std::vector<int> sizes(nproc); auto sizesp = sizes.data();
  std::vector<int> displ(nproc); auto displp = displ.data();
  typed_allgather(&size, 1, sizesp, 1, frovedis_comm_rpc);
  int tot_size = 0; for(int i = 0; i < nproc; ++i) tot_size += sizesp[i];
  displp[0] = 0;
#pragma _NEC novector
  for(int i = 1; i < nproc; ++i) displp[i] = displ[i-1] + sizesp[i-1];
  std::vector<T> gathered_vec(tot_size);
  typed_allgatherv(vec.data(), size,
                   gathered_vec.data(), sizesp, displp,
                   frovedis_comm_rpc);
  //std::cout << "[rank " << get_selfid() << "]: vec: "; debug_print_vector(vec);
  //std::cout << "[rank " << get_selfid() << "]: recvcounts: "; debug_print_vector(sizes);
  //std::cout << "[rank " << get_selfid() << "]: displacements: "; debug_print_vector(displ);
  //std::cout << "[rank " << get_selfid() << "]: gathered: "; debug_print_vector(gathered_vec);
  return gathered_vec;
}

// similar to numpy.ndarray.astype()
template <class R, class T>
std::vector<R>
vector_astype(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  std::vector<R> ret(vecsz);
  auto vptr = vec.data();
  auto rptr = ret.data();
  for(size_t i = 0; i < vecsz; ++i) rptr[i] = static_cast<R>(vptr[i]);
  return ret;
}

// similar to numpy.sum(x)
template <class T>
T vector_sum(const std::vector<T>& vec) {
  T sum = 0;
  auto vecsz = vec.size();
  auto vecp = vec.data();
  for(size_t i = 0; i < vecsz; ++i) sum += vecp[i];
  return sum;
}

// similar to numpy.square(x)
template <class T>
std::vector<T>
vector_square(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  auto vecp = vec.data();
  std::vector<T> ret(vecsz);
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = vecp[i] * vecp[i];
  return ret;
}

// similar to numpy.sqrt(x) for non-integral x
template <class T>
std::vector<T>
vector_sqrt(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  auto vecp = vec.data();
  std::vector<T> ret(vecsz);
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) {
    retp[i] = (vecp[i] == 0) ? 0 : std::sqrt(vecp[i]);
  }
  return ret;
}

template <class T>
void vector_sqrt_inplace(std::vector<T>& vec) {
  auto vecsz = vec.size();
  auto vecp = vec.data();
  for(size_t i = 0; i < vecsz; ++i) {
    vecp[i] = (vecp[i] == 0) ? 0 : std::sqrt(vecp[i]);
  }
}

// similar to numpy.dot(x,x) or numpy.sum(numpy.square(x))
template <class T>
T vector_squared_sum_impl(const std::vector<T>& vec, T& maxval) {
  auto sz = vec.size();
  if (sz == 0) { maxval = 0; return static_cast<T>(0); }
  auto vptr = vec.data();
  // overflow handling
  maxval = std::abs(vptr[0]);
  T zero = static_cast<T>(0);
  for(size_t i = 0; i < sz; ++i) {
    auto absval = vptr[i] * ((vptr[i] >= zero) - (vptr[i] < zero));
    if (absval > maxval) maxval = absval;
  }
  if (maxval == 0) return 0;
  auto one_by_max = static_cast<T>(1.0) / maxval;
  T sqsum = 0.0;
  for(size_t i = 0; i < sz; ++i) {
    auto tmp = vptr[i] * one_by_max; // dividing with max to avoid overflow!
    sqsum += tmp * tmp;
  }
  return sqsum;
}

// similar to numpy.dot(x,x) or numpy.sum(numpy.square(x))
template <class T>
T vector_squared_sum(const std::vector<T>& vec) {
  T maxval = 0;
  auto sqsum_part = vector_squared_sum_impl(vec, maxval);
  return sqsum_part * maxval * maxval;
}

template <>
int vector_squared_sum(const std::vector<int>& vec); // defined in vector_operations.cc

// similar to numpy.linalg.norm(x) -> returns euclidean norm of input vector, x
template <class T>
T vector_norm(const std::vector<T>& vec) {
  T maxval = 0;
  auto sqsum_part = vector_squared_sum_impl(vec, maxval);
  return std::sqrt(sqsum_part) * maxval;
}

// similar to numpy.mean(x)
template <class T>
double vector_mean(const std::vector<T>& vec) {
  return static_cast<double>(vector_sum(vec)) / vec.size();
}

// sum squared difference: similar to numpy.sum(numpy.square(x - y)) or numpy.dot(x - y, x - y)
template <class T>
T vector_ssd(const std::vector<T>& v1,
             const std::vector<T>& v2) {
  auto size = v1.size();
  checkAssumption(size == v2.size());
/*
  auto v1p = v1.data();
  auto v2p = v2.data();
  T sq_error = 0;
  for(size_t i = 0; i < size; ++i) {
    auto error = v1p[i] - v2p[i];
    sq_error += (error * error); // might overflow here...
  }
  return sq_error;
*/
  return vector_squared_sum(v1 - v2); // handles overflow for non-integer vector
}

// sum squared mean difference: similar to numpy.sum(numpy.square(x - numpy.mean(x)))
template <class T>
double vector_ssmd(const std::vector<T>& vec) {
  auto size = vec.size();
  auto vptr = vec.data();
  auto mean = vector_mean(vec);
/*
  double sq_mean_error = 0.0;
  for(size_t i = 0; i < size; ++i) {
    auto error = vptr[i] - mean;
    sq_mean_error += (error * error); // might overflow here...
  }
  return sq_mean_error;
*/
  std::vector<double> error(size); auto eptr = error.data();
  for(size_t i = 0; i < size; ++i) eptr[i] = vptr[i] - mean; 
  return vector_squared_sum(error); // handles overflow for non-integer vector
}

// TODO: support decremental case 10 to 2 etc., negative case -10 to -2 egtc.
// similar to numpy.arange(st, end, step)
template <class T>
std::vector<T>
vector_arrange(const T& st,
               const T& end,
               const T& step = 1) {
  checkAssumption(step != 0);
  if (st >= end && step > 0) return std::vector<T>(); // quick return
  auto sz = ceil_div(end - st, step);
  std::vector<T> ret(sz);
  auto retp = ret.data();
  for(size_t i = 0; i < sz; i += step) retp[i] = st + i;
  return ret;
}

template <class T>
std::vector<T>
vector_arrange(const size_t& end) {
  return vector_arrange<T>(0, end);
}

// similar to numpy.sort(x)
template <class T>
std::vector<T> 
vector_sort(const std::vector<T>& vec,
            bool positive_only = false) {
  if (vec.empty()) return std::vector<T>();
  auto copy_vec = vec; // copying, since radix_sort operates inplace
  radix_sort(copy_vec, positive_only);
  return copy_vec;
}

template <class T, class I>
std::vector<T> 
vector_sort(const std::vector<T>& vec,
            std::vector<I>& pos,
            bool positive_only = false) {
  if (vec.empty()) return std::vector<T>();
  auto copy_vec = vec; // copying, since radix_sort operates inplace
  pos = vector_arrange<I>(vec.size());
  radix_sort(copy_vec, pos, positive_only);
  return copy_vec;
}

// TODO: add vector_count(with condition as function pointer)
// similar to numpy.count_nonzero()
template <class T>
size_t vector_count_nonzero(const std::vector<T>& vec) {
  size_t count = 0;
  auto size = vec.size();
  auto vptr = vec.data();
  for(size_t i = 0; i < size; ++i) count += !vptr[i];
  return size - count;
}

template <class T>
size_t vector_count_equal(const std::vector<T>& vec, const T& val) {
  size_t count = 0;
  auto size = vec.size();
  auto vptr = vec.data();
  for(size_t i = 0; i < size; ++i) count += vptr[i] == val;
  return count;
}

template <class T>
size_t vector_count_positives(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  auto vptr = vec.data();
  size_t count = 0;
  for(size_t i = 0; i < vecsz; ++i) count += vptr[i] > 0;
  return count;
}

template <class T>
size_t vector_count_negatives(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  auto vptr = vec.data();
  size_t count = 0;
  for(size_t i = 0; i < vecsz; ++i) count += vptr[i] < 0;
  return count;
}

// defined in vector_operations.cc
template <>
size_t vector_count_negatives(const std::vector<size_t>& vec);

template <class T>
size_t vector_count_infinite(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  auto vptr = vec.data();
  size_t count = 0;
  for(size_t i = 0; i < vecsz; ++i) count += !std::isfinite(vptr[i]);
  return count;
}

template <class T>
size_t vector_count_out_of_range(const std::vector<T>& vec,
                                 const T& lb, 
                                 const T& ub,
                                 bool is_lb_inclusive=false,
                                 bool is_ub_inclusive=false) {
  auto vecsz = vec.size();
  auto vptr = vec.data();
  size_t count = 0;
  if (is_lb_inclusive && is_ub_inclusive)
    for(size_t i = 0; i < vecsz; ++i) count += (vptr[i] < lb || vptr[i] > ub);
  else if (is_lb_inclusive && !is_ub_inclusive)
    for(size_t i = 0; i < vecsz; ++i) count += (vptr[i] < lb || vptr[i] >= ub);
  else if (is_ub_inclusive && !is_lb_inclusive)
    for(size_t i = 0; i < vecsz; ++i) count += (vptr[i] <= lb || vptr[i] > ub);
  else
    for(size_t i = 0; i < vecsz; ++i) count += (vptr[i] <= lb || vptr[i] >= ub);
  return count;  
}

// similar to numpy.zeros()
template <class T>
std::vector<T> 
vector_zeros(const size_t& size) {
  return std::vector<T>(size); // default initialization of std::vector is with zero
}

// similar to numpy.ones()
template <class T>
std::vector<T> 
vector_ones(const size_t& size) {
  std::vector<T> ret (size);
  auto rptr = ret.data();
  for(size_t i = 0; i < size; ++i) rptr[i] = static_cast<T>(1);
  return ret;
}

// similar to numpy.full()
template <class T>
std::vector<T> 
vector_full(const size_t& size, const T& val) {
  std::vector<T> ret (size);
  auto rptr = ret.data();
  for(size_t i = 0; i < size; ++i) rptr[i] = val;
  return ret;
}

template <class I, class W>
W encode_unique_elements_impl(size_t* sepvalp, I* targetp, size_t i, // input
                              size_t* indp, W* weightp, // input
                              I* unqinvp) { // output
  W weight_sum = 0; // return value
  I enc_val = targetp[i];
  size_t j = sepvalp[i];
  auto nelem = sepvalp[i + 1] - sepvalp[i];
  // loop-expand till length-4 to reduce short loop-length vector performance issue
  if (nelem == 1) {
    auto idx0 = indp[j + 0];
    unqinvp[idx0] = enc_val;
    weight_sum += weightp[idx0];
  }
  else if (nelem == 2) {
    auto idx0 = indp[j + 0];
    auto idx1 = indp[j + 1];
    unqinvp[idx0] = unqinvp[idx1] = enc_val;
    weight_sum += weightp[idx0] + weightp[idx1];
  }
  else if (nelem == 3) {
    auto idx0 = indp[j + 0];
    auto idx1 = indp[j + 1];
    auto idx2 = indp[j + 2];
    unqinvp[idx0] = unqinvp[idx1] = unqinvp[idx2] = enc_val;
    weight_sum += weightp[idx0] + weightp[idx1] + weightp[idx2];
  }
  else if (nelem == 4) {
    auto idx0 = indp[j + 0];
    auto idx1 = indp[j + 1];
    auto idx2 = indp[j + 2];
    auto idx3 = indp[j + 3];
    unqinvp[idx0] = unqinvp[idx1] = enc_val;
    unqinvp[idx2] = unqinvp[idx3] = enc_val;
    weight_sum += weightp[idx0] + weightp[idx1] + weightp[idx2] + weightp[idx3];
  }
  else {
    for(; j < sepvalp[i + 1]; ++j) { 
      auto idx = indp[j];
      unqinvp[idx] = enc_val;
      weight_sum += weightp[idx];
    }
  }
  return weight_sum;
}

template <class I, class W>
std::vector<I> 
encode_unique_elements(std::vector<size_t>& sorted_indices,
                       std::vector<size_t>& unique_sep,
                       std::vector<I>& inverse_target,
                       std::vector<W>& sample_weight,
                       std::vector<W>& unique_weight_sum) {
  auto count = unique_sep.size() - 1;
  auto nelem = sorted_indices.size();

  if(inverse_target.empty()) 
    inverse_target = vector_arrange<I>(count); // for zero-based encoding
  require(inverse_target.size() == count, 
  std::string("vector_unique: size of inverse_target differs with no. of ") +
  std::string("unique samples in input vector!\n"));

  if(sample_weight.empty())
    sample_weight = vector_ones<W>(nelem); // simple count
  require(sample_weight.size() == nelem,
  std::string("vector_unique: size of sample_weight differs with no. of ") +
  std::string("samples in input vector!\n"));
  
  //inputs 
  auto indp = sorted_indices.data();
  auto sepvalp = unique_sep.data();
  auto targetp = inverse_target.data();
  auto weightp = sample_weight.data();

  // outputs
  std::vector<I> unique_inverse(nelem);
  unique_weight_sum.resize(count);
  auto unqinvp = unique_inverse.data();
  auto unqwgtp = unique_weight_sum.data();

  // expanded till length-4 to avoid performance issue with tiny vector loop length
  if (count == 1) {
    auto enc_val = targetp[0];
    auto weight_sum = 0;
    for(size_t i = 0; i < nelem; ++i) {
      unqinvp[i] = enc_val;
      weight_sum += weightp[i];
    }
    unqwgtp[0] = weight_sum;
  }
  else if (count == 2) { // expanded for binary-label case
    size_t i = 0;
    unqwgtp[0] = encode_unique_elements_impl(sepvalp, targetp, i,
                                             indp, weightp, 
                                             unqinvp);
    unqwgtp[1] = encode_unique_elements_impl(sepvalp, targetp, i + 1,
                                             indp, weightp, 
                                             unqinvp);
  }
  else { // multi-label case
    for(size_t i = 0; i < count; ++i) {
      unqwgtp[i] = encode_unique_elements_impl(sepvalp, targetp, i,
                                               indp, weightp, 
                                               unqinvp);
    }
  }
  return unique_inverse;
}

// similar to numpy.unique()
template <class T, class I, class W>
std::vector<T>
vector_unique(const std::vector<T>& vec,
              std::vector<size_t>& unique_indices,
              std::vector<I>& unique_inverse,
              std::vector<size_t>& unique_counts,
              std::vector<I>& inverse_target,
              std::vector<W>& sample_weight,
              std::vector<W>& unique_weight_sum,
              bool positive_only = false,
              bool need_inverse = true) {
  auto vecsz = vec.size();
  if (vecsz == 0) return std::vector<T>(); // quick return
  std::vector<size_t> indices;
  auto sorted = vector_sort(vec, indices, positive_only);
  auto sep = set_separate(sorted);
  auto count = sep.size() - 1;
  std::vector<T> unique(count);
  unique_indices.resize(count);
  unique_counts.resize(count);
  auto sepvalp = sep.data();
  auto unqvalp = unique.data();
  auto unqindp = unique_indices.data();
  auto unqcntp = unique_counts.data();
  auto vecp = sorted.data();
  auto indp = indices.data();
  for(size_t i = 0; i < count; ++i) {
    unqvalp[i] = vecp[sepvalp[i]];
    unqindp[i] = indp[sepvalp[i]];
    unqcntp[i] = sepvalp[i + 1] - sepvalp[i];
  }
  if (need_inverse) {
    unique_inverse = encode_unique_elements(indices, sep, 
                     inverse_target, sample_weight, unique_weight_sum);
  }
  return unique;
}

template <class T, class I, class W>
std::vector<T>
vector_unique(const std::vector<T>& vec,
              std::vector<size_t>& unique_indices,
              std::vector<I>& unique_inverse,
              std::vector<size_t>& unique_counts,
              std::vector<W>& sample_weight,
              std::vector<W>& unique_weight_sum,
              bool positive_only = false) {
  std::vector<I> inverse_target; // for zero-based encoding
  bool need_inverse = true;
  return vector_unique(vec, unique_indices,
                       unique_inverse, unique_counts,
                       inverse_target, sample_weight, unique_weight_sum,
                       positive_only, need_inverse);
}

template <class T, class I>
std::vector<T>
vector_unique(const std::vector<T>& vec,
              std::vector<size_t>& unique_indices,
              std::vector<I>& unique_inverse,
              std::vector<size_t>& unique_counts,
              std::vector<I>& inverse_target, // when encoding target is specified
              bool positive_only = false) {
  std::vector<int> sample_weight, unique_weight_sum; // will be ignored
  bool need_inverse = true;
  return vector_unique(vec, unique_indices,
                       unique_inverse, unique_counts,
                       inverse_target, sample_weight, unique_weight_sum,
                       positive_only, need_inverse);
}

template <class T, class I>
std::vector<T>
vector_unique(const std::vector<T>& vec,
              std::vector<size_t>& unique_indices,
              std::vector<I>& unique_inverse,
              std::vector<size_t>& unique_counts,
              bool positive_only = false) {
  std::vector<I> inverse_target; // for zero-based encoding
  std::vector<int> sample_weight, unique_weight_sum; // will be ignored
  bool need_inverse = true;
  return vector_unique(vec, unique_indices,
                       unique_inverse, unique_counts,
                       inverse_target, sample_weight, unique_weight_sum,
                       positive_only, need_inverse);
}

template <class T>
std::vector<T>
vector_unique(const std::vector<T>& vec,
              std::vector<size_t>& unique_indices,
              std::vector<size_t>& unique_counts,
              bool positive_only = false) {
  std::vector<int> inverse_target, unique_inverse; // will be ignored
  std::vector<int> sample_weight, unique_weight_sum; // will be ignored
  bool need_inverse = false;
  return vector_unique(vec, unique_indices,
                       unique_inverse, unique_counts,
                       inverse_target, sample_weight, unique_weight_sum,
                       positive_only, need_inverse);
}

template <class T>
std::vector<T>
vector_unique(const std::vector<T>& vec,
              bool positive_only = false) {
  return set_unique(vector_sort(vec, positive_only));
}

// similar to numpy.bincount()
template <class R, class I>
std::vector<R> // 'R' can be int, size_t etc...
vector_bincount(const std::vector<I>& vec) { // must be of integer type: int, short, long, size_t etc.
  auto vecsz = vec.size();
  if (vecsz == 0) return std::vector<R>(); // quick return
  auto negatives = vector_count_negatives(vec);
  require(negatives == 0, "bincount: negative element is detected!\n");
  std::vector<size_t> uidx, ucnt;
  bool positive_only = true;
  auto unq = vector_unique(vec, uidx, ucnt, positive_only);
  auto unqsz = unq.size();
  auto uptr = unq.data();
  auto cntptr = ucnt.data();
  auto max = uptr[unqsz - 1]; // unq is a sorted array, last elem should be max
  std::vector<R> ret(max + 1, 0);
  auto rptr = ret.data();
  if (std::is_same<R, size_t>::value)
    for(size_t i = 0; i < unqsz; ++i) rptr[uptr[i]] = cntptr[i];
  else 
    for(size_t i = 0; i < unqsz; ++i) rptr[uptr[i]] = static_cast<R>(cntptr[i]);
  return ret;
}

// similar to numpy.log()
template <class T>
std::vector<double>
vector_log(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  std::vector<double> ret(vecsz);
  auto vecp = vec.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = std::log(vecp[i]);
  return ret;
}

// similar to numpy.divide()
template <class T>
std::vector<T>
vector_divide(const std::vector<T>& v1,
              const std::vector<T>& v2) {
  auto vecsz = v1.size();
  checkAssumption(vecsz == v2.size());
  std::vector<T> ret(vecsz);
  auto v1p = v1.data();
  auto v2p = v2.data();
  auto retp = ret.data();
  int divzero = 0;
  for(size_t i = 0; i < vecsz; ++i) {
    if (v2p[i] == 0) { 
      divzero = 1;
      retp[i] = 0;
    }
    else retp[i] = v1p[i] / v2p[i];
  }
  if(divzero) REPORT_WARNING(WARNING_MESSAGE, 
  "RuntimeWarning: divide by zero encountered in divide");
  return ret;
}

template <class T>
std::vector<T>
vector_divide(const std::vector<T>& vec,
              const T& by_elem) {
  auto vecsz = vec.size();
  if (by_elem == 0) {
    REPORT_WARNING(WARNING_MESSAGE,
        "RuntimeWarning: divide by zero encountered in divide");
    return vector_zeros<T>(vecsz);
  }
  std::vector<T> ret(vecsz);
  auto vecp = vec.data();
  auto retp = ret.data();
  double one_by_elem = 1.0 / by_elem; 
  for(size_t i = 0; i < vecsz; ++i) retp[i] = vecp[i] * one_by_elem;
  return ret;
}

template <>
std::vector<int>
vector_divide(const std::vector<int>& vec,
              const int& by_elem); // defined in vector_operations.cc

template <class T>
std::vector<T>
operator/ (const std::vector<T>& vec,
           const T& by_elem) {
  return vector_divide(vec, by_elem);
}

template <class T>
std::vector<T>
operator/ (const std::vector<T>& v1,
           const std::vector<T>& v2) {
  return vector_divide(v1, v2);
}

// similar to numpy.multiply()
template <class T>
std::vector<T>
vector_multiply(const std::vector<T>& v1,
                const std::vector<T>& v2) {
  auto vecsz = v1.size();
  checkAssumption(vecsz == v2.size());
  std::vector<T> ret(vecsz);
  auto v1p = v1.data();
  auto v2p = v2.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = v1p[i] * v2p[i];
  return ret;
}

template <class T>
std::vector<T>
vector_multiply(const std::vector<T>& v1,
                const T& by_elem) {
  auto vecsz = v1.size();
  std::vector<T> ret(vecsz);
  auto v1p = v1.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = v1p[i] * by_elem;
  return ret;
}

template <class T>
std::vector<T>
operator* (const std::vector<T>& v1,
           const std::vector<T>& v2) {
  return vector_multiply(v1, v2);
}

template <class T>
std::vector<T>
operator* (const std::vector<T>& v1,
           const T& by_elem) {
  return vector_multiply(v1, by_elem);
}

// similar to numpy.add()
template <class T>
std::vector<T>
vector_add(const std::vector<T>& v1,
           const std::vector<T>& v2) {
  auto vecsz = v1.size();
  checkAssumption(vecsz == v2.size());
  std::vector<T> ret(vecsz);
  auto v1p = v1.data();
  auto v2p = v2.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = v1p[i] + v2p[i];
  return ret;
}

template <class T>
std::vector<T>
vector_add(const std::vector<T>& v1,
           const T& by_elem) {
  auto vecsz = v1.size();
  std::vector<T> ret(vecsz);
  auto v1p = v1.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = v1p[i] + by_elem;
  return ret;
}

template <class T>
std::vector<T>
operator+ (const std::vector<T>& v1,
           const std::vector<T>& v2) {
  return vector_add(v1, v2);
}

template <class T>
std::vector<T>
operator+ (const std::vector<T>& v1,
           const T& by_elem) {
  return vector_add(v1, by_elem);
}

// similar to numpy.dot() or blas.dot() - it also supports integer type input vector
template <class T>
T vector_dot(const std::vector<T>& v1,
             const std::vector<T>& v2) {
  auto vecsz = v1.size();
  checkAssumption(vecsz == v2.size());
  auto v1p = v1.data();
  auto v2p = v2.data();
  auto ret = 0;
  for(size_t i = 0; i < vecsz; ++i) ret += v1p[i] * v2p[i];
  return ret;
}

// similar to blas.axpy() - it also supports integer type input vector
template <class T>
std::vector<T>
vector_axpy(const std::vector<T>& v1,
            const std::vector<T>& v2,
            const T& alpha = 1) {
  auto vecsz = v1.size();
  checkAssumption(vecsz == v2.size());
  std::vector<T> ret(vecsz);
  auto v1p = v1.data();
  auto v2p = v2.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = alpha * v1p[i] + v2p[i];
  return ret;
}

// similar to numpy.subtract()
template <class T>
std::vector<T>
vector_subtract(const std::vector<T>& v1,
                const std::vector<T>& v2) {
  auto vecsz = v1.size();
  checkAssumption(vecsz == v2.size());
  std::vector<T> ret(vecsz);
  auto v1p = v1.data();
  auto v2p = v2.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = v1p[i] - v2p[i];
  return ret;
}

template <class T>
std::vector<T>
vector_subtract(const std::vector<T>& v1,
                const T& by_elem) {
  auto vecsz = v1.size();
  std::vector<T> ret(vecsz);
  auto v1p = v1.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = v1p[i] - by_elem;
  return ret;
}

template <class T>
std::vector<T>
operator- (const std::vector<T>& v1,
           const std::vector<T>& v2) {
  return vector_subtract(v1, v2);
}

template <class T>
std::vector<T>
operator- (const std::vector<T>& v1,
           const T& by_elem) {
  return vector_subtract(v1, by_elem);
}

// similar to numpy.negative()
template <class T>
std::vector<T>
vector_negative(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  std::vector<T> ret(vecsz);
  auto vecp = vec.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = -vecp[i];
  return ret;
}

// similar to -numpy.log(x) or numpy.negative(numpy.log(x))
template <class T>
std::vector<double>
vector_negative_log(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  std::vector<double> ret(vecsz);
  auto vecp = vec.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = -std::log(vecp[i]);
  return ret;
}

template <class T>
std::vector<T>
operator-(const std::vector<T>& vec) {
  return vector_negative(vec);
}

// similar to numpy.sum(vec * al) -> vector_scaled_sum(vec, al)
// For numpy.sum(vec / al) -> numpy.sum(vec * (1 / al)) -> vector_scaled_sum(vec, 1 / al)
// TODO: Fix issue for int-type while doing: vector_scaled_sum(vec, 1 / al)
template <class T>
T vector_scaled_sum(const std::vector<T>& vec,
                    const T& al) {
  auto vecsz = vec.size();
  auto vecp = vec.data();
  T sum = 0;
  for(size_t i = 0; i < vecsz; ++i) sum += vecp[i] * al;
  return sum;
}

// similar to numpy.argmax()
template <class T>
size_t vector_argmax(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  require(vecsz > 0, "vector_argmax: input vector is empty!");
  auto vecp = vec.data();
  size_t maxindx = 0;
  T max = std::numeric_limits<T>::lowest();
  for(size_t i = 0; i < vecsz; ++i) {
    if (vecp[i] > max) {
      max = vecp[i];
      maxindx = i;
    }
  }
  return maxindx;
}

// similar to numpy.amax()
template <class T>
T vector_amax(const std::vector<T>& vec) {
  return vec[vector_argmax(vec)]; 
}

// similar to numpy.argmin()
template <class T>
size_t vector_argmin(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  require(vecsz > 0, "vector_argmax: input vector is empty!");
  auto vecp = vec.data();
  size_t minindx = 0;
  T min = std::numeric_limits<T>::max();
  for(size_t i = 0; i < vecsz; ++i) {
    if (vecp[i] < min) {
      min = vecp[i];
      minindx = i;
    }
  }
  return minindx;
}

// similar to numpy.amin()
template <class T>
T vector_amin(const std::vector<T>& vec) {
  return vec[vector_argmin(vec)]; 
}

// similar to numpy.clip()
template <class T>
std::vector<T>
vector_clip(const std::vector<T>& vec,
            const T& min = std::numeric_limits<T>::min(),
            const T& max = std::numeric_limits<T>::max()) {
  checkAssumption(min <= max);
  auto vecsz = vec.size();
  std::vector<T> ret(vecsz);
  auto vecp = vec.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) {
    if (vecp[i] <= min) retp[i] = min;
    else if (vecp[i] >= max) retp[i] = max;
    else retp[i] = vecp[i]; // within (min, max) range
  }
  return ret;
}

// similar to np.take(vec, idx)
template <class R, class T>
std::vector<R>
vector_take(const std::vector<T>& vec,
            const std::vector<size_t>& idx) {
  auto vsz = vec.size();
  require(vsz > 0, "vector_take: input vector is empty!");
  require(idx[vector_argmax(idx)] < vsz,
  "vector_take: idx contains index which is larger than input vector size!");
  auto sz = idx.size();
  std::vector<R> ret(sz);
  auto vecp = vec.data();
  auto idxp = idx.data();
  auto retp = ret.data();
  if (std::is_same<R,T>::value)
    for(size_t i = 0; i < sz; ++i) retp[i] = vecp[idxp[i]];
  else
    for(size_t i = 0; i < sz; ++i) retp[i] = static_cast<R>(vecp[idxp[i]]);
  return ret;
}

// similar to sklearn.preprocessing.binarize()
template <class T>
std::vector<T>
vector_binarize(const std::vector<T>& vec,
                const T& threshold = 0) {
  auto vecsz = vec.size();
  std::vector<T> ret(vecsz);
  auto vecp = vec.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = vecp[i] > threshold;
  return ret;
}

template<class T, class I>
std::vector<T>
get_keys(const std::vector<std::pair<T, I>>& key_val_pair) {
  auto sz = key_val_pair.size();
  std::vector<T> ret(sz);
  auto retp = ret.data();
  auto inputp = key_val_pair.data();
  for(size_t i = 0; i < sz; ++i) retp[i] = inputp[i].first;
  return ret;
}

template<class T, class I>
std::vector<I>
get_values(const std::vector<std::pair<T, I>>& key_val_pair) {
  auto sz = key_val_pair.size();
  std::vector<I> ret(sz);
  auto retp = ret.data();
  auto inputp = key_val_pair.data();
  for(size_t i = 0; i < sz; ++i) retp[i] = inputp[i].second;
  return ret;
}

template<class T, class I>
std::vector<std::pair<T, I>>
make_key_value_pair(const std::vector<T>& key,
                    const std::vector<I>& val) {
  if(key.size() != val.size())
    REPORT_ERROR(USER_ERROR, "key and val size mismatch");
  std::vector<std::pair<T, I>> ret(key.size());
  auto ret_ptr = ret.data();
  auto key_ptr = key.data();
  auto val_ptr = val.data();
  for(size_t i = 0; i < key.size(); i++) {
    ret_ptr[i].first = key_ptr[i];
    ret_ptr[i].second = val_ptr[i];
  }
  return ret;
}

template <class T, class I>
std::vector<std::pair<T, I>>
vector_min_pair(const std::vector<std::pair<T, I>>& t1,
                const std::vector<std::pair<T, I>>& t2) {
  if(t1.size() != t2.size())
    REPORT_ERROR(USER_ERROR, "vectors size do not match");
  auto sz = t1.size();
  std::vector<std::pair<T, I>> res(sz);
  auto res_ptr = res.data();
  for(size_t i = 0; i < sz; ++i)
    res_ptr[i] = t1[i].first <= t2[i].first ? t1[i] : t2[i];
  return res;
}

template <class T, class I>
std::vector<I>
vector_min_index(const std::vector<std::pair<T, I>>& t1,
                 const std::vector<std::pair<T, I>>& t2) {
  if(t1.size() != t2.size())
    REPORT_ERROR(USER_ERROR, "vectors size do not match");
  auto sz = t1.size();
  std::vector<I> res(sz);
  auto res_ptr = res.data();
  for(size_t i = 0; i < sz; ++i)
    res_ptr[i] = t1[i].first <= t2[i].first ? t1[i].second : t2[i].second;
  return res;
}

template <class T, class I>
std::vector<T>
vector_min_value(const std::vector<std::pair<T, I>>& t1,
                 const std::vector<std::pair<T, I>>& t2) {
  if(t1.size() != t2.size())
    REPORT_ERROR(USER_ERROR, "vectors size do not match");
  auto sz = t1.size();
  std::vector<T> res(sz);
  auto res_ptr = res.data();
  for(size_t i = 0; i < sz; ++i)
    res_ptr[i] = t1[i].first <= t2[i].first ? t1[i].first : t2[i].first;
  return res;
}

template <class T, class I>
std::vector<std::pair<T, I>>
vector_max_pair(const std::vector<std::pair<T, I>>& t1,
                const std::vector<std::pair<T, I>>& t2) {
  if(t1.size() != t2.size())
    REPORT_ERROR(USER_ERROR, "vectors size do not match");
  auto sz = t1.size();
  std::vector<std::pair<T, I>> res(sz);
  auto res_ptr = res.data();
  for(size_t i = 0; i < sz; ++i)
    res_ptr[i] = t1[i].first >= t2[i].first ? t1[i] : t2[i];
  return res;
}

template <class T, class I>
std::vector<I>
vector_max_index(const std::vector<std::pair<T, I>>& t1,
                 const std::vector<std::pair<T, I>>& t2) {
  if(t1.size() != t2.size())
    REPORT_ERROR(USER_ERROR, "vectors size do not match");
  auto sz = t1.size();
  std::vector<I> res(sz);
  auto res_ptr = res.data();
  for(size_t i = 0; i < sz; ++i)
    res_ptr[i] = t1[i].first >= t2[i].first ? t1[i].second : t2[i].second;
  return res;
}

template <class T, class I>
std::vector<T>
vector_max_value(const std::vector<std::pair<T, I>>& t1,
                 const std::vector<std::pair<T, I>>& t2) {
  if(t1.size() != t2.size())
    REPORT_ERROR(USER_ERROR, "vectors size do not match");
  auto sz = t1.size();
  std::vector<T> res(sz);
  auto res_ptr = res.data();
  for(size_t i = 0; i < sz; ++i)
    res_ptr[i] = t1[i].first >= t2[i].first ? t1[i].first : t2[i].first;
  return res;
}

template <class T>
T vector_logsumexp_impl(const T* datap,
                        size_t size, size_t stride) {
  auto maxval = std::numeric_limits<T>::lowest();
  for(size_t i = 0; i < size; ++i) {
    if(datap[i * stride] > maxval) maxval = datap[i * stride];
  }
  T lse = 0;
  for(size_t i = 0; i < size; ++i) lse += exp(datap[i * stride] - maxval);
  return maxval + log(lse);
}

// similar to scipy.misc.logsumexp(x)
// T: must be non-integral type
template <class T>
T vector_logsumexp(const std::vector<T>& vec) {
  return vector_logsumexp_impl(vec.data(), vec.size(), 1);
}

// similar to np.exp(x)
// T: must be non-integral type
template <class T>
std::vector<T>
vector_exp(const std::vector<T>& vec) {
  auto vecsz = vec.size();
  std::vector<T> ret(vecsz);
  auto vecp = vec.data();
  auto retp = ret.data();
  for(size_t i = 0; i < vecsz; ++i) retp[i] = exp(vecp[i]);
  return ret;
}

template <class T>
void vector_exp_inplace(std::vector<T>& vec) {
  auto vecsz = vec.size();
  auto vecp = vec.data();
  for(size_t i = 0; i < vecsz; ++i) vecp[i] = exp(vecp[i]);
}

template <class T>
int vector_is_same_impl(const T* vptr1,
                        const T* vptr2,
                        size_t sz1, size_t sz2) {
  if (sz1 != sz2) return false;
  size_t st = 0, end = OP_VLEN, count = 0;
  /*
  for(size_t i = 0; i < sz1; ++i) count += (vptr1[i] == vptr2[i]);
  return count == sz1;
  */
  for(; end < sz1; end += OP_VLEN) {
    count = 0;
    for(size_t i = st; i < end; ++i) count += (vptr1[i] == vptr2[i]);
    if (count != OP_VLEN) return false; // kind of break (but checked after OP_VLEN steps for ve performance)
    st = end;
  }
  count = 0;
  auto rem = sz1 - st;
  if (rem > NOVEC_LEN) {
    for(size_t i = st; i < sz1; ++i) count += (vptr1[i] == vptr2[i]);
  }
  else { // short-loop
    #pragma _NEC novector
    for(size_t i = st; i < sz1; ++i) count += (vptr1[i] == vptr2[i]);
  }
  return count == rem;
}

template <class T>
int vector_is_same(const std::vector<T>& v1,
                   const std::vector<T>& v2) {
  return vector_is_same_impl(v1.data(), v2.data(), v1.size(), v2.size());
}
  
template <class T>
int vector_is_uniform_impl(const T* vptr, size_t vsz) {
  if (vsz == 0) return true;
  T first_val = vptr[0];
  size_t st = 0, end = OP_VLEN, count = 0;
  /*
  for(size_t i = 0; i < vsz; ++i) count += (vptr[i] == first_val);
  return count == vsz;
  */
  for(; end < vsz; end += OP_VLEN) {
    count = 0;
    for(size_t i = st; i < end; ++i) count += (vptr[i] == first_val);
    if (count != OP_VLEN) return false; // kind of break (but checked after OP_VLEN steps for ve performance)
    st = end;
  }
  count = 0;
  auto rem = vsz - st;
  if (rem > NOVEC_LEN) {
    for(size_t i = st; i < vsz; ++i) count += (vptr[i] == first_val);
  }
  else { // short-loop
    #pragma _NEC novector
    for(size_t i = st; i < vsz; ++i) count += (vptr[i] == first_val);
  }
  return count == rem;
}

template <class T>
int vector_is_uniform(const std::vector<T>& vec) {
  return vector_is_uniform_impl(vec.data(), vec.size());
}

template <class T>
std::vector<T>
vector_right_shift(const std::vector<T>& vec, size_t tid) {
  auto vsz = vec.size();
  if (vsz == 0) return std::vector<T>();
  require(tid < vsz, "invalid tid for shift operation is provided!\n");
  std::vector<T> ret(vsz);
  auto vecp = vec.data();
  auto retp = ret.data();
  retp[0] = vecp[tid];
  for(size_t i = tid; i > 0; --i) retp[i] = vecp[i - 1];   // right-shift
  for(size_t i = tid + 1; i < vsz; ++i) retp[i] = vecp[i]; // simple copy
  return ret;
}

template <class T>
void vector_right_shift_inplace(std::vector<T>& vec, size_t tid) {
  auto vsz = vec.size();
  if (vsz == 0) return;
  require(tid < vsz, "invalid tid for shift operation is provided!\n");
  auto vecp = vec.data();
  auto tmp = vecp[tid];
  for(size_t i = tid; i > 0; --i) vecp[i] = vecp[i - 1]; // right-shift
  vecp[0] = tmp;
}

}
#endif
