from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("customer_id").orderBy("balance", "id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id")
w4 = Window.partitionBy("status", "customer_id").orderBy(F.col("balance").desc_nulls_last())
w5 = Window.partitionBy("customer_id")
w6 = Window.partitionBy("status", "customer_id").orderBy(F.col("balance").desc())
w7 = Window.partitionBy("customer_id").orderBy("last_known_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .select(
        "*",
        F.last("balance", ignorenulls=True).over(w).alias("last_known_balance"),
        F.lead("balance", 1).over(w_running).alias("next_balance"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

enriched_accounts = (
    latest
    .select(
        "*",
        F.dense_rank().over(w4).alias("balance_dense_rank"),
        F.avg("balance").over(w5).alias("customer_id_avg_balance"),
        F.rank().over(w6).alias("balance_rank"),
    )
)

result = (
    enriched_accounts
    .select(
        "*",
        F.sum("balance_dense_rank").over(w7).alias("cumulative_balance_dense_rank"),
        F.sum("balance_dense_rank").over(w8).alias("running_balance_dense_rank"),
    )
)
