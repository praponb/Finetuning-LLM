from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.orderBy("opened_date", "id")

with_windows = (
    df_accounts
    .select(
        "*",
        F.avg("balance").over(w_ordered).alias("balance_avg_6"),
        F.lag("balance", 2).over(w_recent).alias("prev_balance"),
    )
)

result = (
    with_windows
    .groupBy("status")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.sum("balance_avg_6") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("balance_avg_6_per_frozen"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("balance_avg_6") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_avg_6_per_closed"),
    )
)
