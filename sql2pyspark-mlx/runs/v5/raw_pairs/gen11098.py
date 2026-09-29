from pyspark.sql import functions as F

result = (
    df_products
    .select(F.lit("product").alias("source"), "price")
    .union(
        df_payments
        .filter(F.col("amount") <= 1530)
        .select(F.lit("payment").alias("source"), "amount")
    )
    .distinct()
)
