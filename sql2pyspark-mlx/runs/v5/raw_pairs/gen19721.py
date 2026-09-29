from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .select("amount")
    .unionByName(df_products.select(F.col("cost").alias("amount")))
    .distinct()
)
