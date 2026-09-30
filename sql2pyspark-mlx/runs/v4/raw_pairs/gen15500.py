from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_recent = Window.partitionBy("customer_id")
w3 = Window.partitionBy("k", "customer_id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

valid = (
    flattened
    .filter(F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 21:00:00")))
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        "value",
        F.when(F.col("k") == "weight_kg", 1).otherwise(0).alias("is_weight_kg"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.credit_limit"))
)

enriched = (
    with_dims
    .withColumn("is_weight_kg_max_30m", F.max("is_weight_kg").over(w))
    .withColumn("customer_id_max_is_weight_kg", F.max("is_weight_kg").over(w_recent))
    .withColumn("k_count_is_weight_kg", F.count("is_weight_kg").over(w3))
)

result = (
    enriched
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "created_at",
        "k",
        "value",
        "credit_limit",
        "is_weight_kg_max_30m",
        "customer_id_max_is_weight_kg",
        F.col("min_order_shipping_fee"),
    )
)
