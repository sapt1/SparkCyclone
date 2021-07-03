#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern "C"  long vector_avg(non_null_double_vector* input, non_null_double_vector* output)
{
    int i;
    int j;

    non_null_double_vector input_data = input[0];
    int row_count = input_data.count/output->count;
#if DEBUG
    printf("Total number of elements received: %d \n", input_data.count);
    printf("Row count of received dataset: %d \n", row_count);
#endif

    output->data = (double*)malloc(output->count * sizeof(double));

    for (i = 0; i < output->count; i++) {
       double sum = 0;
       for(j = 0; j < row_count; j++){
          sum += input_data.data[i + (j * output->count)];
       }

       output->data[i] = sum / row_count;
    }
    
    return 0;
}