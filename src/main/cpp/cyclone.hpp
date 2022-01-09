/*
 * Copyright (c) 2021 Xpress AI.
 *
 * This file is part of Spark Cyclone.
 * See https://github.com/XpressAI/SparkCyclone for further info.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <limits>
#include <iostream>
#include <vector>
#include <chrono>
#include <ctime>
#include <algorithm>
#include "transfer-definitions.hpp"
#include "frovedis/text/dict.hpp"
#include "frovedis/text/words.hpp"
#include "frovedis/text/char_int_conv.hpp"
#include "frovedis/text/parsefloat.hpp"
#include "frovedis/text/parsedatetime.hpp"
#include "frovedis/text/datetime_utility.hpp"

static std::string utcnanotime();
inline void log(std::string msg);
inline void set_validity(uint64_t *validityBuffer, int32_t idx, int32_t validity);
inline uint64_t check_valid(uint64_t *validityBuffer, int32_t idx);
frovedis::words data_offsets_to_words(const char *data, const int32_t *offsets, const int32_t size, const int32_t count);
frovedis::words varchar_vector_to_words(const non_null_varchar_vector *v);
frovedis::words varchar_vector_to_words(const nullable_varchar_vector *v);
void words_to_varchar_vector(frovedis::words& in, nullable_varchar_vector *out);
void debug_words(frovedis::words &in);
std::vector<size_t> idx_to_std(nullable_int_vector *idx);
void print_indices(std::vector<size_t> vec);
frovedis::words filter_words(frovedis::words &in_words, std::vector<size_t> to_select);
