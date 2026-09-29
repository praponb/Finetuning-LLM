from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_transactions
    .withColumn("last3_avg_amount", F.avg("amount").over(w))
    .withColumn("last3_max_amount", F.max("amount").over(w))
    .withColumn("last3_count_amount", F.count("amount").over(w))
)
