from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last())
w3 = Window.partitionBy("channel").orderBy(F.col("discount").desc(), "id")
w_recent = Window.partitionBy("store_id").orderBy("discount", "id")

metrics = (
    df_orders
    .withColumn("cumulative_discount", F.sum("discount").over(w_grp))
    .withColumn("amount_rank", F.rank().over(w2))
    .withColumn("discount_bucket", F.ntile(3).over(w3))
    .withColumn("discount_bucket_2", F.ntile(4).over(w_recent))
)

result = (
    metrics
    .cube(F.date_trunc("year", "order_date").alias("year_start"))
    .agg(
        F.round(F.sum("discount_bucket_2") / F.nullif(F.count(F.when(F.col("status") == "SHIPPED", 1)), F.lit(0)), 2).alias("discount_bucket_2_per_shipped"),
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("status") == "PENDING", F.col("discount")).otherwise(0)).alias("pending_discount"),
    )
    .orderBy(F.col("discount_bucket_2_per_shipped").desc())
)
