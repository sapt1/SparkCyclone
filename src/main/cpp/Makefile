NCC = /opt/nec/ve/bin/ncc
NCC_OPTS = -xc++
ALL = cyclone.so
all: $(ALL)
cyclone.o: cyclone.cc \
	cyclone.hpp \
	frovedis/core/conditions_for_find.hpp \
	frovedis/core/config.hpp \
	frovedis/core/find_condition.hpp \
	frovedis/core/lower_bound.hpp \
	frovedis/core/prefix_sum.hpp \
	frovedis/core/radix_sort.hpp \
	frovedis/core/set_operations.hpp \
	frovedis/core/upper_bound.hpp \
	frovedis/core/utility.cc \
	frovedis/core/utility.hpp \
	frovedis/dataframe/hashtable.hpp \
	frovedis/dataframe/join.cc \
	frovedis/dataframe/join.hpp \
	frovedis/text/char_int_conv.cc \
	frovedis/text/char_int_conv.hpp \
	frovedis/text/datetime_to_words.cc \
	frovedis/text/datetime_to_words.hpp \
	frovedis/text/datetime_utility.hpp \
	frovedis/text/dict.cc \
	frovedis/text/dict.hpp \
	frovedis/text/find.cc \
	frovedis/text/find.hpp \
	frovedis/text/find_condition.hpp \
	frovedis/text/float_to_words.cc \
	frovedis/text/float_to_words.hpp \
	frovedis/text/int_to_words.cc \
	frovedis/text/int_to_words.hpp \
	frovedis/text/parsedatetime.cc \
	frovedis/text/parsedatetime.hpp \
	frovedis/text/parsefloat.cc \
	frovedis/text/parsefloat.hpp \
	frovedis/text/parseint.hpp \
	frovedis/text/words.cc \
	frovedis/text/words.hpp \
	transfer-definitions.hpp \
	tuple_hash.hpp \
	utility.hpp
	$(NCC) $(NCC_OPTS) -o $@ $^

cyclone.so: cyclone.o \
	$(NCC) -shared -pthread -o cyclone.so cyclone.o

clean:
	rm -f cyclone.so