from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "CREDIT", "REFUND"]))
    .groupBy("is_flagged")
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
)
