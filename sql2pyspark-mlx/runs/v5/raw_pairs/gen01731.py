from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("account_type", "customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("balance").asc_nulls_last())
w5 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .select(
        "id",
        "account_type",
        "opened_date",
        F.avg("balance").over(w).alias("balance_avg_6"),
        F.sum("balance").over(w2).alias("running_balance"),
        F.sum("balance").over(w3).alias("cumulative_balance"),
        F.cume_dist().over(w4).alias("balance_cume_dist"),
        F.ntile(10).over(w5).alias("balance_bucket"),
    )
)
