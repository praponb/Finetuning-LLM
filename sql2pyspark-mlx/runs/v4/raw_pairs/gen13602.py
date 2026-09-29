from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_first(), "id")

latest = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = latest.select("id", "account_id", "amount", "txn_time", "merchant", "is_flagged")
