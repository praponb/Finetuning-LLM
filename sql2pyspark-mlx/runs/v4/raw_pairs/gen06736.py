from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 1)
)
