from pyspark.sql import functions as F

result = (
    df_products
    .select("price", F.lit("product").alias("origin"))
    .unionByName(
        df_transactions
        .select(F.col("amount").alias("price"), F.lit("transaction").alias("origin"))
    )
    .distinct()
)
