from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type")
w2 = Window.orderBy("balance")
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("status", "account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.avg("balance").over(w).alias("customer_id_avg_balance"),
        F.cume_dist().over(w2).alias("balance_cume_dist"),
        F.sum("balance").over(w3).alias("running_balance"),
        F.sum("balance").over(w_running).alias("cumulative_balance"),
        F.lead("balance", 1).over(w5).alias("next_balance"),
    )
)
