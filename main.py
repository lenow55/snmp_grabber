from datetime import datetime
from glob import glob
from pyarrow import parquet as pa_pq
from pyarrow import csv
for filename in glob('./out2/*.parquet', recursive=True):
    table = pa_pq.read_table(filename)
    csv.write_csv(table, filename+".csv")
