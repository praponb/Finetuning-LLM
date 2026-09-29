from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant")
w3 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("adjusted_amount", F.round((F.col("amount") * 1.07), 2))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("N/A")))
)

result = (
    cleaned
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("merchant_stddev_samp_amount", F.stddev_samp("amount").over(w2))
    .withColumn("amount_count_7d", F.count("amount").over(w3))
    .withColumn("adjusted_amount_max_90d", F.max("adjusted_amount").over(w4))
)
