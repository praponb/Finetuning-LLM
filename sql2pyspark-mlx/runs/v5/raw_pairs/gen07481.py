from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "session_id",
        F.when(F.col("status").rlike("^(REFUNDED|SUCCESS)$"), "EXTENDED").when(F.col("status").rlike("^(FAILED)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("paid_at").desc_nulls_first())
)
