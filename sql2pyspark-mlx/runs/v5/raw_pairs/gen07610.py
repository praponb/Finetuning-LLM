from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")

deduped = (
    df_device_readings
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("temp_c") >= 40.4, "PREMIUM").otherwise("STANDARD").alias("temp_c_tier"),
    )
    .agg(
        F.percentile_approx("temp_c", 0.9).alias("temp_c_p90"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.corr("status_bits", "pressure").alias("corr_status_bits_pressure"),
        F.count("*").alias("event_count"),
        F.min("reading_ts").alias("first_seen"),
    )
    .filter(F.col("temp_c_p90") < 1000000)
    .orderBy(F.col("duration_min").desc_nulls_last())
)
