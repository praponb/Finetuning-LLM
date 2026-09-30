from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w3 = Window.partitionBy("device_code")
w4 = Window.orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_device_readings
    .select(
        "*",
        F.lag("temp_c", 1).over(w).alias("prev_temp_c"),
        F.row_number().over(w2).alias("row_seq"),
        F.corr("humidity", "status_bits").over(w3).alias("corr_humidity_status_bits"),
        F.sum("temp_c").over(w4).alias("running_temp_c"),
    )
)

result = (
    scored
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .withColumn("humidity_tier", F.when(F.col("humidity") >= 72, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["device_code", "activity_day", "humidity_tier"], ["device_code", "activity_day"]],
        "device_code",
        "activity_day",
        "humidity_tier",
    )
    .agg(
        F.sum("temp_c").alias("total_temp_c"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.avg("humidity").alias("avg_humidity"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("temp_c")).otherwise(0)).alias("dv_02_temp_c"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("distinct_ids") < 1)
    .orderBy(F.col("activity_day").desc_nulls_first())
)
