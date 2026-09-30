from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").desc_nulls_first())

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "status",
        F.min("balance").over(w).alias("last7_min_balance"),
        F.sum("balance").over(w).alias("last7_sum_balance"),
        F.count("balance").over(w).alias("last7_count_balance"),
        F.percent_rank().over(w2).alias("balance_percent_rank"),
    )
)
