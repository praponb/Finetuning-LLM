from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("txn_type").isin(["REFUND", "DEBIT"]) | (F.to_date("txn_time") >= "2023-10-01"))
)

agg = (
    base
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
    )
)

result = (
    agg
    .orderBy("starbucks_count", "account_id")
    .limit(20)
)
