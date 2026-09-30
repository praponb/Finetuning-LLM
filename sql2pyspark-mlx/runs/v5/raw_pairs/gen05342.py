from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy(F.col("merchant").desc())
)
