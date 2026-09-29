from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .select("id", "account_id", "amount", "txn_type")
    .orderBy(F.col("amount").desc())
)
