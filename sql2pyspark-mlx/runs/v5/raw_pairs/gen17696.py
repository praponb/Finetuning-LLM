from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("duration_sec") != 452) & (F.col("duration_sec") == 127)) | F.col("duration_sec").between(130, 143))
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
