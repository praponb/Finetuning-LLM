from pyspark.sql import functions as F

amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("amount").alias("max_order_amount"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

result = (
    exploded
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "created_at", F.col("max_order_amount"))
    .orderBy(F.col("customer_id").desc())
)
