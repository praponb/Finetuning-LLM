from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status", "account_type").orderBy(F.col("balance_whole").asc_nulls_first())
w2 = Window.partitionBy("status").orderBy("balance", "id")
w3 = Window.partitionBy("account_type", "status").orderBy(F.col("balance_whole").desc_nulls_first())
w4 = Window.partitionBy("status").orderBy("balance_whole_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("balance_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("dedup_rank", "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-06-06").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.ceil("balance").alias("balance_whole"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w_hist).alias("balance_whole_dense_rank"),
        F.ntile(10).over(w2).alias("balance_bucket"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

with_windows_accounts = (
    latest
    .withColumn("cumulative_balance_whole_dense_rank", F.sum("balance_whole_dense_rank").over(w4))
    .withColumn("first_known_balance_bucket", F.first("balance_bucket", ignorenulls=True).over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
)

result = (
    with_windows_accounts
    .groupBy(
        F.when(F.col("balance") >= 80000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        "seq_no",
    )
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("seq_no", [0.25, 0.5, 0.75]).alias("seq_no_quartiles"),
        F.percentile_approx("balance_whole", 0.9).alias("balance_whole_p90"),
    )
    .filter(F.col("num_status") >= 1)
    .orderBy(F.col("seq_no").desc_nulls_first())
)
