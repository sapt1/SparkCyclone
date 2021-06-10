#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <parquet-glib/parquet-glib.h>

long sum_vectors(non_null_double_vector* input, non_null_double_vector* output)
{
    int i;
    int j;

    non_null_double_vector input_data = input[0];
    int row_count = input_data.count/output->count;

#if DEBUG
    printf("Total number of elements received: %d \n", input_data.count);
    printf("Row count of received dataset: %d \n", row_count);
#endif
    output->data = malloc(output->count * sizeof(double));
       GError *err = NULL;
         GParquetArrowFileReader * d = gparquet_arrow_file_reader_new_path("/root/d/sample.parquet", &err);
         GArrowSchema* s = gparquet_arrow_file_reader_get_schema(d, &err);
               printf("DATA: %s",garrow_schema_to_string(s));
               printf("ERR: %s", err->message);

    #pragma omp parallel for
    for (i = 0; i < output->count; i++) {
       double sum = 0;
       for(j = 0; j < row_count; j++){
          sum += input_data.data[i + (j * output->count)];
       }

       output->data[i] = sum;
    }
    
    return 0;
}
