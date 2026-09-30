from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("QUARTER", "prev_reading_ts").alias("quarter_start"))
    .agg(
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("session_id_per_dv_04"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
    )
    .orderBy(F.col("session_id_per_dv_04").desc_nulls_first())
)
