from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2024-02-13").cast("date")) & (F.col("paid_at") < "2023-12-01"))
    .select("*", F.to_date("paid_at").alias("paid_at_date"))
)

with_dims = (
    base
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.order_date"), F.col("b.status").alias("order_status"))
)

with_gaps = (
    with_dims
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

wide = (
    islands
    .groupBy("order_id", "method")
    .pivot("status", ["FAILED", "REFUNDED"])
    .agg(F.sum("amount"))
)

result = (
    wide
    .select(
        "order_id",
        "method",
        "FAILED",
        "REFUNDED",
        F.when(F.col("method").rlike("^(bank_transfer|cash)$"), "GROUP_B").when(F.col("method").rlike("^(card|paypal)$"), "PRIORITY").otherwise("EXTENDED").alias("method_zone"),
    )
    .orderBy(F.col("FAILED").asc_nulls_last())
)
