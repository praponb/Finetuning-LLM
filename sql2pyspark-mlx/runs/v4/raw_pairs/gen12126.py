from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("cost")
    .unionByName(
        df_orders
        .filter(F.col("discount") < 0.3)
        .select(F.col("amount").alias("cost"))
    )
    .distinct()
)
