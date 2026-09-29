from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        "method",
        F.when(F.col("amount") >= 1230, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("method") == "promptpay", 1)), F.lit(0)), 2).alias("is_new_burst_per_promptpay"),
        F.min("paid_at").alias("first_seen"),
    )
    .orderBy(F.col("is_new_burst_per_promptpay").desc_nulls_first())
)
