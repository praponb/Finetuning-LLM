from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & F.col("device_code").isin(["DV-02", "DV-04", "DV-03"]))
    .withColumn("humidity_whole", F.ceil("humidity"))
)

gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("island_id") >= 7, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.corr("temp_c", "pressure").alias("corr_temp_c_pressure"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
    )
    .orderBy(F.col("island_id_tier").asc_nulls_last())
)
