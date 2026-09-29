from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.ntile(10).over(w2).alias("amount_bucket"),
    )
)
