from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNull())
    .select(F.col("cost").alias("value"), F.lit("product").alias("source"))
    .unionByName(
        df_transactions
        .select(F.col("amount").alias("value"), F.lit("transaction").alias("source"))
    )
    .distinct()
)
