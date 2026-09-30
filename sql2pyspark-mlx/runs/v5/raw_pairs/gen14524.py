from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("running_sum_amount").desc_nulls_last(), "id")

cleaned = (
    df_orders
    .filter((~F.col("channel").eqNullSafe(F.lit("mobile"))) & (F.col("discount") < 0.22))
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.lower("status").alias("status_lower"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.count("amount").over(w_recent).alias("running_count_amount"),
        F.sum("amount").over(w_recent).alias("running_sum_amount"),
        F.avg("amount").over(w_recent).alias("running_avg_amount"),
    )
)

base = (
    enriched
    .filter(F.col("channel").isNotNull())
    .withColumn("discount_rounded", F.round("discount", 2))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "COMPLETED", 1)).alias("completed_count"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("running_count_amount")).otherwise(0)).alias("cancelled_running_count_amount"),
        F.count("*").alias("event_count"),
        F.avg(F.when(F.col("status") == "RETURNED", F.col("running_avg_amount"))).alias("returned_avg_running_avg_amount"),
        F.count("*").alias("event_count_2"),
    )
    .orderBy("cancelled_running_count_amount")
)
