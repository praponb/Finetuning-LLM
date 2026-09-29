from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.when(F.col("amount") >= 1410, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.countDistinct("method").alias("distinct_methods"),
        F.sum("is_new_burst").alias("total_is_new_burst"),
        F.avg("amount").alias("avg_amount"),
        F.count("*").alias("row_count"),
    )
)
