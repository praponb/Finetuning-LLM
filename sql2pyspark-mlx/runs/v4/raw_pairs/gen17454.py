from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_first(), "id")

deduped = (
    df_transactions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    deduped
    .withColumn("amount_tier", F.when(F.col("amount") >= 3300, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .groupingSets(
        [["amount_tier", "year_start", "activity_month"], ["amount_tier", "year_start"]],
        "amount_tier",
        "year_start",
        "activity_month",
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount_2"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
        F.max(F.when(F.col("txn_type") == "FEE", F.col("amount"))).alias("fee_max_amount"),
        F.count_if(F.col("merchant") == "Lazada").alias("lazada_events"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("lazada_events") < 1)
)
