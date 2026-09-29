from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").asc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w3 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first())
w5 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_recent = Window.partitionBy("device_code").orderBy("status_bits")
w_grp = Window.partitionBy("device_code")
w8 = Window.partitionBy("device_code").orderBy("temp_c_bucket", "id")
w9 = Window.partitionBy("device_code").orderBy("status_bits", "id")

deduped = (
    df_device_readings
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "device_code",
        "humidity",
        "status_bits",
        "firmware",
        "rank_in_group",
        F.ntile(3).over(w3).alias("temp_c_bucket"),
        F.dense_rank().over(w4).alias("pressure_dense_rank"),
        F.avg("humidity").over(w5).alias("humidity_avg_1h"),
    )
)

enriched_device_readings = (
    enriched
    .filter(F.col("status_bits") <= 7)
    .select(
        "id",
        "device_code",
        "status_bits",
        "firmware",
        "rank_in_group",
        "temp_c_bucket",
        "pressure_dense_rank",
        "humidity_avg_1h",
        F.rank().over(w_recent).alias("status_bits_rank"),
        F.avg("status_bits").over(w_grp).alias("device_code_avg_status_bits"),
        F.ntile(10).over(w8).alias("temp_c_bucket_bucket"),
        F.lead("status_bits", 2).over(w9).alias("next_status_bits"),
    )
)

result = (
    enriched_device_readings
    .withColumn("next_status_bits_tier", F.when(F.col("next_status_bits") >= 9, "PREMIUM").otherwise("STANDARD"))
    .withColumn("temp_c_bucket_bucket_tier", F.when(F.col("temp_c_bucket_bucket") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("status_bits_tier", F.when(F.col("status_bits") >= 10, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["next_status_bits_tier", "temp_c_bucket_bucket_tier", "status_bits_tier"], ["next_status_bits_tier", "temp_c_bucket_bucket_tier"], []],
        "next_status_bits_tier",
        "temp_c_bucket_bucket_tier",
        "status_bits_tier",
    )
    .agg(
        F.round(F.sum("temp_c_bucket") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("temp_c_bucket_per_dv_01"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("status_bits_rank")).otherwise(0)).alias("dv_02_status_bits_rank"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("temp_c_bucket_per_dv_01") >= 0)
    .orderBy(F.col("temp_c_bucket_bucket_tier").desc())
)
