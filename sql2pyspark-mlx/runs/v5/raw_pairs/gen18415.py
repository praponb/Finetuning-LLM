from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select(
        "*",
        F.count("balance").over(w).alias("last3_count_balance"),
        F.max("balance").over(w).alias("last3_max_balance"),
        F.avg("balance").over(w).alias("last3_avg_balance"),
        F.min("balance").over(w).alias("last3_min_balance"),
        F.ntile(4).over(w2).alias("balance_bucket"),
    )
)
