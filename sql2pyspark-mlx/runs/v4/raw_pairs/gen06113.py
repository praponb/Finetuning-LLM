from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type").orderBy(F.col("txn_time").desc_nulls_first(), "id")

deduped = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select("id", "account_id", "txn_type", "amount", "txn_time", "rn")
    .orderBy(F.col("account_id").asc_nulls_first())
)
