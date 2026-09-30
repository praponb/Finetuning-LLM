from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_last())
w2 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("balance").desc_nulls_first())
w_grp = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        F.cume_dist().over(w).alias("balance_cume_dist"),
        F.sum("balance").over(w2).alias("running_balance"),
        F.percent_rank().over(w3).alias("balance_percent_rank"),
        F.avg("balance").over(w_grp).alias("balance_avg_3"),
    )
)
