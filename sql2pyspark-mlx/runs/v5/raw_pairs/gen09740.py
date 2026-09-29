from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.orderBy(F.col("balance").desc())
w3 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "CLOSED"]))
    .select(
        "id",
        "customer_id",
        "status",
        F.cume_dist().over(w).alias("balance_cume_dist"),
        F.rank().over(w2).alias("balance_rank"),
        F.ntile(3).over(w3).alias("balance_bucket"),
    )
)
