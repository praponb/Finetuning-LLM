from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")
w2 = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "*",
        F.ntile(10).over(w).alias("amount_bucket"),
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(4).over(w3).alias("amount_bucket_2"),
    )
)
