from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 17) & (F.col("txn_type").isin(["CREDIT", "REFUND"]) | (F.col("txn_type") == "CREDIT")))
)

summary = (
    src
    .groupBy("account_id", "is_flagged")
    .agg(
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
        F.count("*").alias("row_count"),
    )
)

result = (
    summary
    .filter(F.col("row_count") < 6)
    .select("account_id", "is_flagged", "grab_amount", "row_count")
    .orderBy(F.col("row_count").desc())
)
