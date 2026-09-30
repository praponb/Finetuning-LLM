from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        F.max("balance").over(w).alias("running_max_balance"),
        F.min("balance").over(w).alias("running_min_balance"),
        F.avg("balance").over(w).alias("running_avg_balance"),
        F.sum("balance").over(w).alias("running_sum_balance"),
    )
)
