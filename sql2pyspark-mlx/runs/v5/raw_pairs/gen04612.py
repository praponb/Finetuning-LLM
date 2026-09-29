from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-05-13").cast("date")) & F.col("order_id").isNotNull())
    .select("*", F.date_format("paid_at", "MMM yyyy").alias("paid_at_label"))
)

flagged = (
    filtered
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "paid_at_label",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

with_dims = (
    islands
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id").eqNullSafe(F.col("o.id")), "left")
    .filter(F.col("o.status") != "SHIPPED")
    .select("s.id", "s.order_id", "s.session_no", F.col("o.order_date"))
)

kept_keys = (
    with_dims
    .select("order_id")
    .intersect(
        df_payments
        .filter(F.to_date("paid_at") < "2025-09-01")
        .select("order_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.date_trunc("week", "order_date").alias("week_start"),
        "order_id",
        F.date_trunc("quarter", "order_date").alias("quarter_start"),
    )
    .agg(
        F.max("session_no").alias("max_session_no"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no_2"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no_3"),
        F.percentile_approx("session_no", [0.25, 0.5, 0.75]).alias("session_no_quartiles"),
    )
    .filter(F.col("max_session_no") > 0)
)
