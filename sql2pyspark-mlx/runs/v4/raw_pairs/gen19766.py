from pyspark.sql import functions as F

src = df_transactions.filter(F.hour("txn_time").between(10, 13))

summary = (
    src
    .groupBy("merchant")
    .agg(F.countDistinct("amount").alias("unique_txn_types"))
)

result = (
    summary
    .orderBy(F.col("unique_txn_types").desc(), "merchant")
    .limit(10)
)
