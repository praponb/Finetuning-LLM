from pyspark.sql import functions as F

result = (
    df_transactions
    .select(F.lit("transaction").alias("source"), "amount")
    .unionByName(
        df_store_sales
        .filter(F.year("sale_date") >= 2025)
        .select(F.lit("store_sale").alias("source"), F.col("revenue").alias("amount"))
    )
    .distinct()
    .orderBy(F.col("amount").desc())
)
