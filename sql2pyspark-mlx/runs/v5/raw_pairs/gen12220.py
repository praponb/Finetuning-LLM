from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id")

with_windows = (
    df_accounts
    .withColumn("event_seq", F.row_number().over(w_part))
    .withColumn("running_balance", F.sum("balance").over(w2))
    .withColumn("prev_balance", F.lag("balance", 1).over(w3))
)

result = (
    with_windows
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.round(F.avg("running_balance"), 2).alias("avg_running_balance"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.max("prev_balance").alias("max_prev_balance"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
    )
    .filter(F.col("max_prev_balance") >= 0)
)
