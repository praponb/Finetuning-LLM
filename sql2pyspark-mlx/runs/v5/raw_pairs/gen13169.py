from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([30, 31]))
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"), "is_flagged")
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
    .orderBy(F.col("txn_month").desc())
)
