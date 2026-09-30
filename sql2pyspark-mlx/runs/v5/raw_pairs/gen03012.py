from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").asc_nulls_first(), "id")
w2 = Window.partitionBy("amount_tier").orderBy(F.col("min_amount").desc())

deduped = (
    df_transactions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("amount") >= 3600, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("amount").alias("min_amount"),
    )
    .withColumn("min_amount_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("amount_tier").desc())
)
