from pyspark.sql import functions as F

result = (
    df_products
    .select("cost", F.lit("product").alias("origin"))
    .unionByName(
        df_orders
        .filter(F.col("order_date") > "2024-01-05")
        .select(F.col("amount").alias("cost"), F.lit("order").alias("origin"))
    )
    .distinct()
)
