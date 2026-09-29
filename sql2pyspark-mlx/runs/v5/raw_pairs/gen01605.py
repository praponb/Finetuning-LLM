from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "DEBIT"]))
    .groupBy("txn_type")
    .agg(F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"))
    .orderBy(F.col("txn_type").desc())
    .limit(5)
)
