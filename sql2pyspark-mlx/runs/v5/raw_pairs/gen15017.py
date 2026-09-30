from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .select(
        "id",
        "order_id",
        "method",
        "burst_id",
        F.when(F.col("method").rlike("^(paypal|card)$"), "GROUP_B").when(F.col("method").rlike("^(bank_transfer|cash)$"), "OTHER_ZONE").otherwise("CORE").alias("method_zone"),
    )
    .orderBy(F.col("method_zone").desc())
)
