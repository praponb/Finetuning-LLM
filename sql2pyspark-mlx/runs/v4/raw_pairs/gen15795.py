from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount_int", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("method", "order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("paid_at").asc_nulls_last(), "id")

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2024-05-01 22:00:00"))) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.order_date"))
)

enriched_payments = (
    enriched
    .select(
        "*",
        F.ntile(3).over(w).alias("amount_int_bucket"),
        F.lead("amount_filled", 1).over(w2).alias("next_amount_filled"),
        F.sum("amount_int").over(w3).alias("amount_int_sum_90d"),
    )
)

flagged = (
    enriched_payments
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_id", F.sum("is_new_burst").over(w4))

deduped = (
    sessions_payments
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select("id", "status", "next_amount_filled", "amount_filled")
    .unpivot(
        ["id", "status"],
        ["next_amount_filled", "amount_filled"],
        "metric",
        "reading",
    )
)
