from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "status").orderBy("balance", "id")
w3 = Window.partitionBy("status").orderBy("balance", "id")
w4 = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .select(
        "*",
        F.last("status", ignorenulls=True).over(w).alias("last_known_status"),
        F.ntile(4).over(w2).alias("balance_bucket"),
        F.row_number().over(w3).alias("event_seq"),
        F.lead("balance", 1).over(w4).alias("next_balance"),
        F.sum("balance").over(w).alias("cumulative_balance"),
    )
)
