from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-2, -1)

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.ntile(4).over(w).alias("balance_bucket"),
        F.avg("balance").over(w2).alias("balance_avg_2_prior"),
    )
)
