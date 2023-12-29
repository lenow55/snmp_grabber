from datetime import datetime
from glob import glob
from pyarrow import parquet as pa_pq
for filename in glob('./output/*.parquet', recursive=True):
    print(pa_pq.read_table(filename))
