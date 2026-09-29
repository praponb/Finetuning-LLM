from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "session_id",
        F.when(F.col("method").rlike("^(bank_transfer|promptpay)$"), "EXTENDED").when(F.col("method").rlike("^(card|paypal)$"), "OTHER_ZONE").otherwise("CORE").alias("method_zone"),
    )
    .orderBy(F.col("paid_at").desc_nulls_first())
)
