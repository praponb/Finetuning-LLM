from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("temp_c").desc(), "id")
w_hist = Window.orderBy("running_min_temp_c", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("humidity", "id")
w_ordered = Window.orderBy("running_min_temp_c", "id")
w6 = Window.orderBy("last7_avg_running_min_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter(~F.col("device_code").isin(["DV-01", "DV-03"]))
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.when(F.col("device_code").isin(["DV-02", "DV-03"]), "Core").otherwise("Other").alias("device_code_group"),
        F.round("pressure", 2).alias("pressure_rounded"),
    )
)

cleaned = (
    valid
    .filter(F.col("temp_c") > 38)
    .withColumn("humidity_times_temp_c", (F.col("humidity") * F.col("temp_c")))
)

scored = (
    cleaned
    .select(
        "id",
        "humidity",
        "device_code_group",
        F.count("temp_c").over(w_part).alias("running_count_temp_c"),
        F.min("temp_c").over(w_part).alias("running_min_temp_c"),
        F.avg("temp_c").over(w_part).alias("running_avg_temp_c"),
        F.ntile(4).over(w2).alias("temp_c_bucket"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "device_code_group",
        "running_min_temp_c",
        F.max("running_min_temp_c").over(w_hist).alias("last7_max_running_min_temp_c"),
        F.min("running_min_temp_c").over(w_hist).alias("last7_min_running_min_temp_c"),
        F.avg("running_min_temp_c").over(w_hist).alias("last7_avg_running_min_temp_c"),
        F.lag("running_avg_temp_c", 2).over(w4).alias("prev_running_avg_temp_c"),
    )
)

enriched = (
    metrics
    .withColumn("next_prev_running_avg_temp_c", F.lead("prev_running_avg_temp_c").over(w_ordered))
    .withColumn("prev_prev_running_avg_temp_c", F.lag("prev_running_avg_temp_c").over(w_ordered))
    .withColumn("next_last7_avg_running_min_temp_c", F.lead("last7_avg_running_min_temp_c").over(w_ordered))
    .withColumn("prev_last7_avg_running_min_temp_c", F.lag("last7_avg_running_min_temp_c").over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("first_known_last7_avg_running_min_temp_c", F.first("last7_avg_running_min_temp_c", ignorenulls=True).over(w6))
)

result = (
    enriched
    .select(
        "id",
        "device_code_group",
        "last7_max_running_min_temp_c",
        "last7_min_running_min_temp_c",
        "last7_avg_running_min_temp_c",
        "prev_running_avg_temp_c",
        "prev_prev_running_avg_temp_c",
        "prev_last7_avg_running_min_temp_c",
        "event_seq",
    )
    .orderBy(F.col("last7_max_running_min_temp_c").asc_nulls_first())
)
