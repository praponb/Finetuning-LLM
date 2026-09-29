from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.lit("store_sale").alias("kind"), F.col("revenue").alias("value"))
    .unionByName(
        df_orders
        .select(F.lit("order").alias("kind"), F.col("amount").alias("value"))
    )
    .distinct()
)
