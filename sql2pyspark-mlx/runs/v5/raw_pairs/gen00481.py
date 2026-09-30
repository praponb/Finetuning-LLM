from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("merchant") == "Lazada") & (F.col("merchant") == "Amazon") & (F.col("txn_time") < "2024-03-14"))
    .groupBy("merchant")
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
    .orderBy(F.col("merchant").desc())
)
