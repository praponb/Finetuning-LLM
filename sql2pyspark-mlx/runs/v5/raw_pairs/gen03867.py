from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status")
w2 = Window.orderBy(F.col("amount").desc(), "id")
w3 = Window.orderBy("is_cash", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("amount", "id")

valid = (
    df_payments
    .filter((~F.col("method").eqNullSafe(F.lit("promptpay"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.when(F.col("method") == "cash", 1).otherwise(0).alias("is_cash"),
        F.substring("status", 1, 4).alias("status_prefix"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "is_cash",
        "resolved_method",
        F.sum("is_cash").over(w).alias("order_id_sum_is_cash"),
        F.ntile(5).over(w2).alias("amount_bucket"),
        F.avg("amount").over(w3).alias("amount_avg_2"),
        F.ntile(5).over(w4).alias("amount_bucket_2"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.discount"), F.col("b.status").alias("order_status"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id").eqNullSafe(F.col("p.id")), "left")
    .filter(F.col("p.shipping_fee").isNotNull())
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.paid_at",
        "e.is_cash",
        "e.resolved_method",
        "e.amount_bucket",
        "e.amount_avg_2",
        "e.amount_bucket_2",
        "e.discount",
        "e.order_status",
        F.col("p.status").alias("order_status_2"),
        F.col("p.amount"),
    )
)

result = (
    with_dims
    .groupBy("order_status", "order_status_2", "method")
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.sum(F.when(F.col("order_status_2") == "COMPLETED", F.col("is_cash")).otherwise(0)).alias("completed_is_cash"),
        F.sum(F.when(F.col("order_status_2") == "SHIPPED", F.col("amount")).otherwise(0)).alias("shipped_amount"),
        F.percentile_approx("amount_avg_2", [0.25, 0.5, 0.75]).alias("amount_avg_2_quartiles"),
        F.size(F.collect_set("order_status")).alias("num_order_status"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
    )
    .orderBy(F.col("completed_is_cash").desc_nulls_first())
)
