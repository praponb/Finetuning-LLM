from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy(F.col("amount").asc_nulls_first())
w2 = Window.partitionBy("method", "status")
w3 = Window.partitionBy("order_id").orderBy("amount", "id")
w4 = Window.orderBy(F.col("amount").desc(), "id")

metrics = (
    df_payments
    .select(
        "*",
        F.cume_dist().over(w_ordered).alias("amount_cume_dist"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.row_number().over(w3).alias("seq_no"),
        F.ntile(3).over(w4).alias("amount_bucket"),
    )
)

result = (
    metrics
    .groupBy("seq_no")
    .agg(
        F.countDistinct("method").alias("distinct_methods"),
        F.round(F.avg("amount_bucket"), 2).alias("avg_amount_bucket"),
        F.max("paid_at").alias("last_seen"),
    )
)
