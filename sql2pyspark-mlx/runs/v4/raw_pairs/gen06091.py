from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "session_start_flag",
        "island_id",
        F.when(F.col("status").rlike("^(SUCCESS|REFUNDED)$"), "GROUP_B").when(F.col("status").rlike("^(FAILED)$"), "CORE").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
