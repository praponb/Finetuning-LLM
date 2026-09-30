from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("amount") >= 1450, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("method") == "card", 1)), F.lit(0)), 2).alias("burst_id_per_card"),
        F.max("paid_at").alias("last_paid_at"),
        F.sum(F.when(F.col("method") == "paypal", F.col("burst_id")).otherwise(0)).alias("paypal_burst_id"),
    )
    .filter(F.col("burst_id_per_card") > 0)
    .orderBy(F.col("burst_id_per_card").desc_nulls_first())
)
