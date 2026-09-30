from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("burst_id", "id")

base = (
    df_payments
    .filter(F.col("method") != "bank_transfer")
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
    )
)

gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

latest = (
    sessionized
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    latest
    .select(
        "id",
        "method",
        "paid_at",
        "status",
        "paid_at_label",
        "burst_id",
        F.when(F.col("status").rlike("^(SUCCESS|FAILED)$"), "EXTENDED").when(F.col("status").rlike("^(REFUNDED)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
)
