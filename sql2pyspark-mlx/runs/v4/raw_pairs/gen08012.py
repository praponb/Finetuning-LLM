from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc(), "id")

ranked = (
    df_device_readings
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy("device_code")
    .agg(
        F.stddev_samp("temp_c").alias("temp_c_stddev"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
    )
    .orderBy("span_minutes")
)
