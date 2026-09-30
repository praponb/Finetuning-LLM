from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_grp = Window.orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("amount_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_id", "txn_type").orderBy("txn_time", "id")

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-11-01 00:00:00").cast("timestamp")) & F.col("txn_type").isin(["REFUND", "DEBIT", "FEE"]))
    .withColumn("amount_scaled", F.round((F.col("amount") * 0.9), 2))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("NONE")))
)

with_windows = (
    base
    .withColumn("amount_scaled_sum_90d", F.sum("amount_scaled").over(w_recent))
    .withColumn("amount_avg_3", F.avg("amount").over(w_grp))
    .withColumn("last_known_amount_scaled", F.last("amount_scaled", ignorenulls=True).over(w3))
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
)

joined = (
    with_windows
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.opened_date"))
)

result = (
    joined
    .groupBy("merchant")
    .agg(
        F.percentile_approx("row_seq", 0.9).alias("row_seq_p90"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount_scaled")).otherwise(0)).alias("debit_amount_scaled"),
    )
    .orderBy(F.col("row_seq_p90").desc_nulls_first())
)
