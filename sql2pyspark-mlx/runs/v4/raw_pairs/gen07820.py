from pyspark.sql import functions as F

result = (
    df_products
    .select("price")
    .unionByName(df_transactions.select(F.col("amount").alias("price")))
    .distinct()
)
