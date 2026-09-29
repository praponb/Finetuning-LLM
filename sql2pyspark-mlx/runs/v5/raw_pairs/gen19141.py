from pyspark.sql import functions as F

result = (
    df_products
    .select("cost", F.lit("product").alias("origin"))
    .union(
        df_payments
        .filter(F.col("amount") >= 1070)
        .select("amount", F.lit("payment").alias("origin"))
    )
    .orderBy("cost")
)
