from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(((F.col("txn_time") < "2023-04-01") & F.col("is_flagged")) | ~F.col("is_flagged"))
    .groupBy("merchant")
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
)
