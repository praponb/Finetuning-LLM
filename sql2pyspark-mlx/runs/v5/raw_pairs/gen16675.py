from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("id")
    .subtract(
        df_device_readings
        .filter(F.col("reading_ts") < "2025-07-01")
        .select("id")
    )
)

kept_rows = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .rollup("device_code", "activity_day")
    .agg(
        F.max(F.when(F.col("device_code") == "DV-04", F.col("humidity"))).alias("dv_04_max_humidity"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.max("reading_ts").alias("ended_at"),
        F.count_if(F.col("device_code") == "DV-03").alias("dv_03_events"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("dv_04_max_humidity").desc())
)
