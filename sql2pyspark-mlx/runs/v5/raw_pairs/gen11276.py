from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("status_bits", "id")

metrics = (
    df_device_readings
    .select(
        "*",
        F.max("pressure").over(w_grp).alias("running_max_pressure"),
        F.count("pressure").over(w_grp).alias("running_count_pressure"),
        F.sum("pressure").over(w_grp).alias("running_sum_pressure"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    metrics
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .rollup("activity_day")
    .agg(
        F.corr("status_bits", "pressure").alias("corr_status_bits_pressure"),
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.min("reading_ts").alias("started_at"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("grouping_level"),
    )
)
