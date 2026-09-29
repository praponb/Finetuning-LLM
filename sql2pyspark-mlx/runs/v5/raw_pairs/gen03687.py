from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "plan").orderBy(F.col("end_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("status").orderBy(F.col("running_avg_is_cancelled_minus_running_sum_is_cancelled").desc(), "id")
w_part = Window.partitionBy("status").orderBy("running_avg_is_cancelled_minus_running_sum_is_cancelled")
w5 = Window.partitionBy("status").orderBy("running_min_is_cancelled", "id")

base = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-09-30").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("start_date_label", F.date_format("start_date", "yyyy-MM-dd"))
    .withColumn("is_cancelled", F.when(F.col("status") == "CANCELLED", 1).otherwise(0))
)

enriched = (
    base
    .filter(F.col("customer_id") == 17)
    .withColumn("running_avg_is_cancelled", F.avg("is_cancelled").over(w_hist))
    .withColumn("running_max_is_cancelled", F.max("is_cancelled").over(w_hist))
    .withColumn("running_sum_is_cancelled", F.sum("is_cancelled").over(w_hist))
    .withColumn("running_min_is_cancelled", F.min("is_cancelled").over(w_hist))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    latest
    .filter(F.col("customer_id").isNotNull() & F.month("start_date").isin([4, 8, 12]))
    .select(
        "id",
        "status",
        "start_date_label",
        "is_cancelled",
        "running_max_is_cancelled",
        "running_min_is_cancelled",
        (F.col("running_avg_is_cancelled") - F.col("running_sum_is_cancelled")).alias("running_avg_is_cancelled_minus_running_sum_is_cancelled"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "status",
        F.dense_rank().over(w_part).alias("running_avg_is_cancelled_minus_running_sum_is_cancelled_dense_rank"),
        F.lead("running_min_is_cancelled", 1).over(w5).alias("next_running_min_is_cancelled"),
    )
)

valid = (
    with_windows
    .filter(~F.col("status").isin(["CANCELLED", "ACTIVE"]))
    .select(
        "*",
        F.round((F.col("next_running_min_is_cancelled") * 1.07), 2).alias("adjusted_next_running_min_is_cancelled"),
        F.when(F.col("next_running_min_is_cancelled") >= 65, "High").when(F.col("next_running_min_is_cancelled") >= 33, "Medium").otherwise("Low").alias("next_running_min_is_cancelled_level"),
    )
)

result = (
    valid
    .select(
        "id",
        "status",
        "running_avg_is_cancelled_minus_running_sum_is_cancelled_dense_rank",
        "next_running_min_is_cancelled_level",
    )
)
