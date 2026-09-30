from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("account_type")

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

joined = (
    cleaned
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .select("src.*", F.col("a.status"), F.col("a.account_type"))
)

metrics = (
    joined
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("amount_avg_3", F.avg("amount").over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
)

result = (
    metrics
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.date_trunc("MONTH", "txn_time").alias("activity_month"),
        F.when(F.col("amount_share") >= 0.73, "HIGH").otherwise("LOW").alias("amount_share_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.min("amount_avg_3").alias("min_amount_avg_3"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("running_amount")).otherwise(0)).alias("checking_running_amount"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("min_amount_avg_3") >= 0)
)
