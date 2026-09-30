from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("amount", "id")
w4 = Window.orderBy("amount", "id")
w5 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("amount").desc_nulls_first())

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select("*", F.lower("txn_type").alias("txn_type_lower"))
)

result = (
    cleaned
    .withColumn("amount_bucket", F.ntile(5).over(w))
    .withColumn("amount_avg_6", F.avg("amount").over(w2))
    .withColumn("prev_amount", F.lag("amount", 1).over(w3))
    .withColumn("next_amount", F.lead("amount", 1).over(w4))
    .withColumn("amount_cume_dist", F.cume_dist().over(w5))
)
