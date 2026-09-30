from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .filter(F.col("opened_date") < "2024-01-01")
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.max("balance").over(w).alias("last4_max_balance"),
        F.avg("balance").over(w).alias("last4_avg_balance"),
        F.min("balance").over(w).alias("last4_min_balance"),
        F.count("balance").over(w).alias("last4_count_balance"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
    )
)
