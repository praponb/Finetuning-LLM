from pyspark.sql import functions as F

src = (
    df_transactions
    .filter((F.col("amount") >= 2800) & F.col("txn_type").isin(["CREDIT", "FEE", "REFUND"]))
)

agg = (
    src
    .groupBy("merchant")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("amount").alias("max_amount"),
    )
)

result = agg.select("merchant", "avg_amount", "max_amount")
