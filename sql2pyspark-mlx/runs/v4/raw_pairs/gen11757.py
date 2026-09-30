from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount", F.lit("payment").alias("source"))
    .unionByName(
        df_products
        .filter(F.col("category") != "Electronics")
        .select(F.col("price").alias("amount"), F.lit("product").alias("source"))
    )
    .distinct()
)
