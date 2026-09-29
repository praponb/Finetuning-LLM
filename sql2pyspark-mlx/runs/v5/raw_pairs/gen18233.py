from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        F.last("status").over(w).alias("last_status"),
        F.percent_rank().over(w2).alias("balance_pct_rank"),
    )
)
