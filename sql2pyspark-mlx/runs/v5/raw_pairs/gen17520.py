from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(~F.col("merchant").isin(["Grab", "Lazada", "Starbucks"]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 2)
    .select("id", "merchant", "txn_time", "amount")
)
