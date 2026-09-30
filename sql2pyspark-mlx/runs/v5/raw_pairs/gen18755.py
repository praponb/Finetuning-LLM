from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("shipping_fee").alias("value"))
    .unionByName(
        df_products
        .filter(F.col("launch_date") <= "2024-10-01")
        .select(F.col("cost").alias("value"))
    )
    .distinct()
)
