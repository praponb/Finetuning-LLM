from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("account_id").isin([5, 19, 26, 30]))
    .withColumn("merchant_lower", F.lower("merchant"))
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 3500, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("txn_type") == "REFUND", 1)), F.lit(0)), 2).alias("amount_per_refund"),
    )
)
