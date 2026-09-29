from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").desc(), "id")
w_ordered = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.ntile(5).over(w2).alias("balance_bucket"),
        F.first("balance", ignorenulls=True).over(w_ordered).alias("first_known_balance"),
    )
)
