from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 460)
    .select("amount")
    .unionByName(
        df_products
        .filter(F.col("cost") <= 710)
        .select(F.col("cost").alias("amount"))
    )
    .distinct()
    .orderBy(F.col("amount").desc())
)
