from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("amount").desc(), F.col("id").desc())
)
