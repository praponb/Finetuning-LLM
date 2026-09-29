from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy(F.col("shipping_fee").asc_nulls_last())
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("channel").orderBy(F.col("amount").desc())
w5 = Window.partitionBy("store_id")

result = (
    df_orders
    .withColumn("shipping_fee_rank", F.rank().over(w_recent))
    .withColumn("amount_dense_rank", F.dense_rank().over(w2))
    .withColumn("shipping_fee_avg_3_prior", F.avg("shipping_fee").over(w3))
    .withColumn("amount_percent_rank", F.percent_rank().over(w4))
    .withColumn("corr_amount_discount", F.corr("amount", "discount").over(w5))
)
