from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.orderBy(F.col("amount").desc_nulls_last())
w3 = Window.orderBy("amount", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_payments
    .filter(F.col("order_id").isin([8, 23, 36, 37]))
    .withColumn("amount_avg_6", F.avg("amount").over(w))
    .withColumn("amount_rank", F.rank().over(w_hist))
    .withColumn("amount_avg_6_2", F.avg("amount").over(w3))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "amount_avg_6",
        "amount_rank",
        F.when(F.col("status").rlike("^(FAILED|SUCCESS)$"), "GROUP_B").when(F.col("status").rlike("^(REFUNDED)$"), "EXTENDED").otherwise("GROUP_A").alias("status_zone"),
    )
    .orderBy(F.col("status").desc_nulls_last(), "paid_at")
)
