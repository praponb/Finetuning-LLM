from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.avg("balance").over(w_ordered).alias("running_avg_balance"),
        F.count("balance").over(w_ordered).alias("running_count_balance"),
        F.sum("balance").over(w_ordered).alias("running_sum_balance"),
        F.ntile(5).over(w2).alias("balance_bucket"),
    )
)
