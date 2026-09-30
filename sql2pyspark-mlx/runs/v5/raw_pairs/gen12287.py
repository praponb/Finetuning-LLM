from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_payments
    .withColumn("last4_avg_amount", F.avg("amount").over(w))
    .withColumn("last4_min_amount", F.min("amount").over(w))
    .withColumn("last4_count_amount", F.count("amount").over(w))
    .withColumn("last4_max_amount", F.max("amount").over(w))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "last4_min_amount",
        "last4_max_amount",
    )
    .orderBy(F.col("last4_max_amount").desc())
)
