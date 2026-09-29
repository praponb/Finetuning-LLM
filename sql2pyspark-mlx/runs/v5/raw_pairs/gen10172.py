from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("customer_id", "account_type").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("balance") >= 32000)
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_6"),
        F.ntile(5).over(w2).alias("balance_bucket"),
    )
)
