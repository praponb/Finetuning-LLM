from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select("shipping_fee", F.lit("order").alias("kind"))
    .union(
        df_products
        .filter(F.col("launch_date").between("2024-07-01", "2025-04-15"))
        .select("cost", F.lit("product").alias("kind"))
    )
    .distinct()
)
