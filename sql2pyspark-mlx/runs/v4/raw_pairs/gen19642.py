from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("amount").desc(), "id")
w4 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

scored = (
    df_payments
    .withColumn("status_min_amount", F.min("amount").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("amount_bucket", F.ntile(4).over(w3))
    .withColumn("amount_count_7d", F.count("amount").over(w4))
)

result = (
    scored
    .groupBy("method")
    .agg(
        F.percentile_approx("amount_bucket", 0.9).alias("amount_bucket_p90"),
        F.percentile_approx("amount_count_7d", 0.95).alias("amount_count_7d_p95"),
        F.max("paid_at").alias("ended_at"),
    )
    .orderBy("amount_bucket_p90")
)
