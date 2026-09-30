from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_transactions
    .select(
        "*",
        F.rank().over(w).alias("amount_rank"),
        F.ntile(10).over(w2).alias("amount_bucket"),
        F.avg("amount").over(w3).alias("amount_avg_3"),
    )
)
