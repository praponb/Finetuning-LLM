from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("balance", "id")
w4 = Window.partitionBy("status", "customer_id").orderBy(F.col("row_seq").desc(), "id")
w5 = Window.partitionBy("status", "account_type").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("row_seq_2", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("activity_month").orderBy(F.col("row_seq_avg_3_per_credit").desc())

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (~F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .select(
        "*",
        F.concat("account_type", F.lit(" - "), "status").alias("account_type_status"),
    )
)

metrics = (
    cleaned
    .filter(F.col("account_type").isin(["CHECKING", "CREDIT"]))
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.ntile(10).over(w4).alias("row_seq_bucket"),
        F.avg("row_seq").over(w5).alias("row_seq_avg_2"),
        F.avg("row_seq").over(w6).alias("row_seq_avg_3"),
    )
)

matched_keys = (
    scored
    .select("customer_id", "id")
    .subtract(
        df_accounts
        .filter(F.col("balance") < 69000)
        .select("customer_id", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.max(F.when(F.col("status") == "FROZEN", F.col("row_seq_avg_3"))).alias("frozen_max_row_seq_avg_3"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.round(F.sum("row_seq_avg_3") / F.nullif(F.count(F.when(F.col("account_type") == "CREDIT", 1)), F.lit(0)), 2).alias("row_seq_avg_3_per_credit"),
    )
    .withColumn("row_seq_avg_3_per_credit_percent_rank", F.percent_rank().over(w7))
)
