from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "method").orderBy(F.col("island_id").desc(), "id")

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

joined = (
    sessions_payments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.discount"))
)

latest = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    latest
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.method",
        "src.status",
        "src.island_id",
        "src.rank_in_group",
        F.col("b.order_date"),
    )
)

result = with_dims.select("id", "order_id", "rank_in_group", "order_date")
