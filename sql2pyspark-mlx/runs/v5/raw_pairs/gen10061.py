from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc_nulls_last())
w4 = Window.partitionBy("customer_id", "segment").orderBy(F.col("shipping_fee").desc_nulls_last(), "id")

kept = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price") > 350), "left_anti")
)

enriched = (
    kept
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id").eqNullSafe(F.col("b.id")), "left")
    .filter(F.col("b.segment") == "Consumer")
    .select(
        "src.id",
        "src.customer_id",
        "src.status",
        "src.channel",
        "src.amount",
        "src.shipping_fee",
        F.col("b.age"),
        F.col("b.city"),
    )
)

enriched_orders = (
    enriched
    .select(
        "id",
        "customer_id",
        "status",
        "shipping_fee",
        "age",
        "city",
        F.row_number().over(w).alias("seq_no"),
        F.corr("amount", "shipping_fee").over(w2).alias("corr_amount_shipping_fee"),
    )
)

with_dims = (
    enriched_orders
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.shipping_fee",
        "s.city",
        F.col("d.segment"),
        F.col("d.credit_limit"),
    )
)

latest = (
    with_dims
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

summary = (
    deduped
    .groupBy("segment", "city")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("credit_limit")).otherwise(0)).alias("tokyo_credit_limit"),
        F.stddev_samp("shipping_fee").alias("shipping_fee_stddev"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
    )
)

result = (
    summary
    .select(
        "segment",
        "city",
        "credit_limit_p50",
        "singapore_count",
        "credit_limit_p90",
    )
    .orderBy("singapore_count", "credit_limit_p50")
)
