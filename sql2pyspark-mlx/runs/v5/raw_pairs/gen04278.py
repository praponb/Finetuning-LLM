from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.first("balance", ignorenulls=True).over(w).alias("first_known_balance"),
        F.sum("balance").over(w2).alias("running_balance"),
    )
)
