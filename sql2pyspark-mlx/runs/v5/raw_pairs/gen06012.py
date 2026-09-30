from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", "coupon_codes", F.inline("items"))
)

result = (
    flattened
    .cube("customer_id", F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum("product_id").alias("total_product_id"),
    )
)
