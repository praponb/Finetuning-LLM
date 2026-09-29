from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("txn_time", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_type").orderBy(F.col("amount_bucket").desc(), "id")
w7 = Window.orderBy("amount_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .select("e.*", F.col("a.account_type"))
)

scored = (
    with_dims
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_2_prior"),
        F.ntile(4).over(w2).alias("amount_bucket"),
    )
)

cleaned = (
    scored
    .filter((F.col("txn_time") > F.lit("2023-08-06").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("txn_time_weekofyear", F.weekofyear("txn_time"))
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w3).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w4))

result = (
    sessionized
    .withColumn("cumulative_is_new_burst", F.sum("is_new_burst").over(w5))
    .withColumn("amount_bucket_bucket", F.ntile(4).over(w6))
    .withColumn("last_known_amount_bucket", F.last("amount_bucket", ignorenulls=True).over(w7))
)
