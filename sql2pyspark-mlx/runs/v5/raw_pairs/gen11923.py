from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-07-07 09:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .withColumn("txn_type_merchant_key", F.concat_ws("/", "txn_type", "merchant"))
    .withColumn("amount_scaled", F.round((F.col("amount") * 1.15), 2))
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_time",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_transactions
    .groupBy("burst_id")
    .agg(
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("burst_id").desc_nulls_first())
)
