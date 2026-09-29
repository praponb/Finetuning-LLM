from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .select(
        "id",
        "paid_at",
        "status",
        "new_group_flag",
        "session_id",
        F.when(F.col("status").rlike("^(FAILED|REFUNDED)$"), "PRIORITY").when(F.col("status").rlike("^(SUCCESS)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("new_group_flag").desc_nulls_last())
)
