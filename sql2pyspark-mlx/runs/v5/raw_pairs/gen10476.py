from pyspark.sql import functions as F

result = (
    df_products
    .select("cost", F.lit("product").alias("origin"))
    .union(df_transactions.select("amount", F.lit("transaction").alias("origin")))
    .distinct()
    .orderBy(F.col("origin").desc())
)
