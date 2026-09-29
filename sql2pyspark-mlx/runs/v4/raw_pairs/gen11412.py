from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").asc_nulls_last(), "id")

latest = (
    df_transactions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .select("id", "account_id", "amount", "txn_time", "is_flagged")
    .orderBy(F.col("txn_time").desc_nulls_first())
)
