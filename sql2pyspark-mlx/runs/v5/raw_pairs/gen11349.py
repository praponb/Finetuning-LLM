from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.lit("order").alias("origin"), F.col("amount").alias("value"))
    .unionByName(
        df_products
        .select(F.lit("product").alias("origin"), F.col("price").alias("value"))
    )
    .orderBy("value")
)
