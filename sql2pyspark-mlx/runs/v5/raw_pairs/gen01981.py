from pyspark.sql import functions as F

kept_keys = (
    df_device_readings
    .select("device_code", "firmware")
    .intersect(
        df_device_readings
        .filter(~F.col("device_code").isin(["DV-03", "DV-01", "DV-04"]))
        .select("device_code", "firmware")
    )
)

verified = (
    df_device_readings
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("device_code", F.date_trunc("DAY", "reading_ts").alias("activity_day"))
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("pressure")).otherwise(0)).alias("dv_01_pressure"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("activity_day").desc())
)
