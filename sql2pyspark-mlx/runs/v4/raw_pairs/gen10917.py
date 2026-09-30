from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.orderBy(F.col("is_amazon").desc(), "id")
w_running = Window.orderBy(F.col("is_amazon_avg_90d").desc(), "id")
w5 = Window.orderBy("first_known_is_amazon", "id")

cleaned = (
    df_transactions
    .filter(F.col("account_id").isin([15, 35, 39]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("merchant") == "Amazon", 1).otherwise(0).alias("is_amazon"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

scored = (
    cleaned
    .withColumn("first_known_is_amazon", F.first("is_amazon", ignorenulls=True).over(w_grp))
    .withColumn("is_amazon_avg_90d", F.avg("is_amazon").over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

enriched = (
    scored
    .withColumn("is_amazon_avg_90d_bucket", F.ntile(3).over(w_running))
    .withColumn("event_seq_2", F.row_number().over(w5))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .select("e.*", F.col("a.opened_date"))
)

result = (
    with_dims
    .groupBy("txn_type")
    .pivot("merchant", ["7-Eleven", "Shell"])
    .agg(F.max("is_amazon_avg_90d").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "txn_type",
        F.col("7-Eleven_value_sum").alias("v_7_eleven_value_sum"),
        F.col("7-Eleven_cnt").alias("v_7_eleven_cnt"),
        F.col("Shell_value_sum").alias("shell_value_sum"),
        F.col("Shell_cnt").alias("shell_cnt"),
    )
)
