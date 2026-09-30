from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.max("amount").alias("max_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("max_amount") > 2300)
    .select("merchant", "max_amount", "amount_count")
    .orderBy(F.col("merchant").desc(), F.col("max_amount").desc())
    .limit(1)
)
