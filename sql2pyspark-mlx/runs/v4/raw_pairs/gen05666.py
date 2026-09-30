from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("status_length").desc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w3 = Window.partitionBy("order_id").orderBy("status_length", "id")
w4 = Window.partitionBy("method").orderBy("status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method", "order_id").orderBy("next_status_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("paid_at_date", "id")

base = (
    df_payments
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-12-17")))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.to_date("paid_at").alias("paid_at_date"),
        F.length("status").alias("status_length"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

deduped = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.lag("status_length", 1).over(w2).alias("prev_status_length"),
        F.lead("status_length", 1, 0).over(w3).alias("next_status_length"),
        F.sum("status_length").over(w4).alias("running_status_length"),
    )
)

enriched = (
    metrics
    .withColumn("running_running_status_length", F.sum("running_status_length").over(w5))
    .withColumn("cumulative_prev_status_length", F.sum("prev_status_length").over(w4))
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy("order_id")
    .agg(
        F.percentile_approx("running_running_status_length", 0.95).alias("running_running_status_length_p95"),
        F.min("paid_at").alias("started_at"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.percentile_approx("next_status_length", 0.5).alias("next_status_length_p50"),
        F.sum(F.when(F.col("method") == "paypal", F.col("running_status_length")).otherwise(0)).alias("paypal_running_status_length"),
    )
    .orderBy(F.col("running_running_status_length_p95").asc_nulls_first())
)
