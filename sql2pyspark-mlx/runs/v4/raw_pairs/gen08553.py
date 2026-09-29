from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("amount") > 2800))
    .select(
        "*",
        F.when(F.col("amount") >= 3600, "High").when(F.col("amount") >= 1900, "Medium").otherwise("Low").alias("amount_level"),
        F.concat_ws("/", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
)

gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("new_group_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("new_group_flag_tier"),
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.avg("new_group_flag").alias("avg_new_group_flag"),
    )
    .filter(F.col("avg_new_group_flag") < 0)
    .orderBy(F.col("new_group_flag_tier").desc())
)
