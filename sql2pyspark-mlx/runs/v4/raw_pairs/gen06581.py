from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        F.col("item.product_id"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "created_at", F.col("min_order_shipping_fee"))
    .orderBy(F.col("customer_id").desc_nulls_last())
)
