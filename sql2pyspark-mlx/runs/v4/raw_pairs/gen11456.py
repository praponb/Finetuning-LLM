from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(~F.col("is_flagged") | F.col("account_id").isin([1, 8, 20, 21]))
)

agg = (
    valid_transactions
    .groupBy(
        F.year("txn_time").alias("txn_year"),
        F.when(F.col("txn_type").isin(["DEBIT", "CREDIT"]), "priority").otherwise("other").alias("txn_type_group"),
    )
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
)

result = (
    agg
    .select("txn_year", "txn_type_group")
    .orderBy(F.col("txn_year").desc())
)
