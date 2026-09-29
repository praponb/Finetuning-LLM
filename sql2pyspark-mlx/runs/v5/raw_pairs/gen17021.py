from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .cube("burst_id", "device_code")
    .agg(
        F.round(F.sum("temp_c") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("temp_c_per_dv_01"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("burst_id")).otherwise(0)).alias("dv_01_burst_id"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("temp_c")).otherwise(0)).alias("dv_03_temp_c"),
        F.max("pressure").alias("max_pressure"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("temp_c_per_dv_01")
)
