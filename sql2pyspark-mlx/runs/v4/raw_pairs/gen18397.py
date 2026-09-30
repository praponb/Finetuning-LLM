from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("paid_at").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w5 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-05-24").cast("date")) & F.col("order_id").isNotNull() & (F.col("status") == "REFUNDED"))
    .select(
        "*",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

base_payments = (
    deduped
    .filter((F.col("paid_at") >= F.lit("2023-11-09 14:00:00").cast("timestamp")) & (F.col("status") == "FAILED"))
    .withColumn("clean_method", F.trim("method"))
)

gaps = (
    base_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_no", F.sum("is_new_burst").over(w3))

gaps_2 = (
    sessions_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = gaps_2.withColumn("burst_id", F.sum("new_group_flag").over(w5))

result = (
    sessionized
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status_method_key",
        "resolved_method",
        "clean_method",
        "session_no",
        "new_group_flag",
    )
)
