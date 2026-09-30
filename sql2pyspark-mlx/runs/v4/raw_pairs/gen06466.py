from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy(F.col("opened_date_minus_49d").desc(), "id")
w5 = Window.partitionBy("status", "customer_id").orderBy("last_known_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("balance_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("opened_date_minus_49d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-07-16").cast("date"))
    .withColumn("status_group", F.when(F.col("status").isin(["CLOSED", "FROZEN"]), "Priority").otherwise("Other"))
    .withColumn("opened_date_minus_49d", F.date_sub("opened_date", 49))
)

with_windows = (
    base
    .withColumn("customer_id_min_balance", F.min("balance").over(w))
    .withColumn("balance_avg_5", F.avg("balance").over(w2))
    .withColumn("customer_id_stddev_samp_balance", F.stddev_samp("balance").over(w))
    .withColumn("last_known_balance", F.last("balance", ignorenulls=True).over(w3))
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows_accounts = (
    latest
    .select(
        "*",
        F.sum("balance").over(w5).alias("cumulative_balance"),
        F.first("balance", ignorenulls=True).over(w6).alias("first_known_balance"),
        F.sum("balance_avg_5").over(w7).alias("running_balance_avg_5"),
    )
)

result = (
    with_windows_accounts
    .groupBy("status")
    .agg(
        F.count("*").alias("row_count"),
        F.max(F.when(F.col("status") == "CLOSED", F.col("balance_avg_5"))).alias("closed_max_balance_avg_5"),
        F.round(F.avg("running_balance_avg_5"), 2).alias("avg_running_balance_avg_5"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_rows"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
    )
    .filter(F.col("closed_max_balance_avg_5") >= 0)
    .orderBy("savings_count")
)
