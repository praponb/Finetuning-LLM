from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount")
    .unionByName(df_products.select(F.col("price").alias("amount")))
)
