from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Amazon") & ~F.col("txn_type").isin(["CREDIT", "REFUND", "FEE"]))
    .select("id", "is_flagged", F.ceil("amount").alias("amount_whole"))
    .orderBy(F.col("amount_whole").desc())
)
