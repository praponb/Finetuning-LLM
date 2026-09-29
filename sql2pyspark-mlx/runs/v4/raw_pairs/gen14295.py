from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

matched_keys = (
    sessions_device_readings
    .select("id", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("humidity") <= 75)
        .select("id", "device_code")
    )
)

kept_rows = (
    sessions_device_readings
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

kept_keys = (
    kept_rows
    .select("id")
    .exceptAll(
        df_device_readings
        .filter(F.col("device_code") == "DV-04")
        .select("id")
    )
)

reconciled = (
    kept_rows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .cube(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.percentile_approx("pressure", [0.25, 0.5, 0.75]).alias("pressure_quartiles"),
        F.round(F.sum("temp_c") / F.nullif(F.count(F.when(F.col("device_code") == "DV-02", 1)), F.lit(0)), 2).alias("temp_c_per_dv_02"),
    )
)
