from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date").between("2023-04-01", "2024-06-01"))
    .select("launch_date", F.lit("product").alias("source"))
    .union(
        df_orders
        .filter(F.col("discount") <= 0.11)
        .select("order_date", F.lit("order").alias("source"))
    )
    .orderBy("source")
)
