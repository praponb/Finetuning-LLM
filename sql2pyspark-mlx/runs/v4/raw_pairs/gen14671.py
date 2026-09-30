from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("method").orderBy(F.col("amount").asc_nulls_last())
w3 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w7 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_payments
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
        F.avg("amount").over(w3).alias("amount_avg_90d"),
        F.last("amount", ignorenulls=True).over(w4).alias("last_known_amount"),
        F.sum("amount").over(w5).alias("cumulative_amount"),
    )
)

enriched = (
    scored
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.amount").alias("order_amount"), F.col("p.discount"))
)

gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w6) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("is_new_session").over(w7))

result = (
    islands
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "row_seq",
        "amount_dense_rank",
        "amount_avg_90d",
        "last_known_amount",
    )
    .orderBy(F.col("last_known_amount").desc_nulls_last())
)
