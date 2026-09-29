from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .select(
        "id",
        "status",
        F.when(F.col("status").rlike("^(REFUNDED|SUCCESS)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy("status_zone")
)
