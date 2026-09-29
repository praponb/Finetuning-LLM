from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("customer_id", "status")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        F.last("balance", ignorenulls=True).over(w).alias("last_known_balance"),
        F.sum("balance").over(w_grp).alias("customer_id_sum_balance"),
    )
)
