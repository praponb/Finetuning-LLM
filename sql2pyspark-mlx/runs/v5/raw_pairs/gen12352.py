from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("customer_id", "status").orderBy("balance")

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "status",
        "customer_id",
        F.first("status").over(w).alias("first_status"),
        F.percent_rank().over(w2).alias("balance_pct_rank"),
    )
    .orderBy("status", "first_status")
)
