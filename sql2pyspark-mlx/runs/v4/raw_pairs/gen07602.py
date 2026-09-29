from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")

result = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "txn_type", "amount")
    .orderBy("txn_type")
)
