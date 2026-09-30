from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("amount") >= 2400)

agg = (
    recent_transactions
    .groupBy("merchant")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)

result = (
    agg
    .orderBy(F.col("merchant").desc())
    .limit(10)
)
