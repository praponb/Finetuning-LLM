from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("amount").alias("value"))
    .unionByName(df_products.select(F.col("cost").alias("value")))
    .distinct()
    .orderBy("value")
)
