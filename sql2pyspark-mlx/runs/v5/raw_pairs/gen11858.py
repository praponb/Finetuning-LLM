from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "store_id").orderBy(F.col("order_date").desc(), "id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

cleaned = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-06-07").cast("date")) & F.col("channel").isin(["web", "store"]))
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        F.add_months("order_date", 6).alias("order_date_plus_months"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.product_id").isin([3, 31])), "left_semi")
)

not_flagged_orders = (
    not_flagged
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < F.col("s.order_date_plus_months")), "left_semi")
)

deduped = (
    not_flagged_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "amount",
        "order_date_plus_months",
        "rank_in_group",
        F.col("max_order_shipping_fee"),
    )
    .orderBy("store_id", F.col("rank_in_group").desc())
)
