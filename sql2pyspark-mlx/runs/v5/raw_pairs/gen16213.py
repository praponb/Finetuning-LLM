from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull() & F.col("method").isin(["card", "paypal"]))
    .withColumn("is_success", F.when(F.col("status") == "SUCCESS", 1).otherwise(0))
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

filtered_payments = (
    sessions_payments
    .filter((F.col("paid_at") > F.lit("2023-10-20 19:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "method", "status").alias("method_status_key"),
        F.coalesce("method", F.lit("NONE")).alias("method_clean"),
    )
)

cleaned = (
    filtered_payments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "is_new_burst",
        "island_id",
        "method_status_key",
        F.round("amount", 2).alias("amount_rounded"),
        F.coalesce("method", F.lit("OTHER")).alias("method_clean_2"),
    )
)

cleaned_payments = (
    cleaned
    .filter(F.col("paid_at") >= F.lit("2023-09-02").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "is_new_burst",
        "island_id",
        "method_clean_2",
        F.trim("status").alias("clean_status"),
    )
)

result = (
    cleaned_payments
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "island_id",
        "method_clean_2",
        "clean_status",
        F.when(F.col("method").rlike("^(bank_transfer|paypal)$"), "PRIORITY").otherwise("GROUP_A").alias("method_zone"),
    )
    .orderBy("method")
)
