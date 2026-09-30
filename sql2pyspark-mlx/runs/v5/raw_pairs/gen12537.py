from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

valid = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") == 35))
    .select(
        "*",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_filled"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.shipping_fee"))
)

scored = (
    with_dims
    .withColumn("running_avg_shipping_fee", F.avg("shipping_fee").over(w))
    .withColumn("running_sum_shipping_fee", F.sum("shipping_fee").over(w))
    .withColumn("running_count_shipping_fee", F.count("shipping_fee").over(w))
    .withColumn("running_max_shipping_fee", F.max("shipping_fee").over(w))
)

matched_keys = (
    scored
    .select("order_id", "id")
    .intersect(
        df_shipments
        .filter(F.col("delivered_date").between("2024-06-22", "2025-03-22"))
        .select("order_id", "id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.amount"),
        F.col("p.shipping_fee").alias("order_shipping_fee"),
        F.col("p.order_date"),
    )
)

result = (
    enriched
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "shipped_date",
        "cost",
        "status",
        "carrier_filled",
        "shipping_fee",
        "running_sum_shipping_fee",
        "running_max_shipping_fee",
        F.col("sum_payment_amount"),
    )
    .orderBy(F.col("sum_payment_amount").desc_nulls_first(), "order_id")
)
