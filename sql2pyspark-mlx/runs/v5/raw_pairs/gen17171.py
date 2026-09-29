from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id")

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-10-10 12:00:00").cast("timestamp")) & (F.col("method") != "cash"))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.upper("method").alias("method_upper"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_clean"),
    )
)

base_payments = (
    base
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-08-16")))
    .select("*", F.coalesce("method", F.lit("N/A")).alias("method_filled"))
)

with_windows = (
    base_payments
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.first("method", ignorenulls=True).over(w2).alias("first_known_method"),
        F.first("method", ignorenulls=True).over(w3).alias("first_known_method_2"),
    )
)

gaps = (
    with_windows
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

joined = (
    sessionized
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.method",
        "e.paid_at",
        "e.method_filled",
        "e.first_known_method",
        "e.first_known_method_2",
        "e.burst_id",
        F.col("o.status"),
        F.col("o.order_date"),
        F.col("o.amount"),
    )
)

result = (
    joined
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["channel", "method"], ["channel"], []],
        F.col("p.channel").alias("channel"),
        F.col("a.method").alias("method"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.amount")).alias("avg_amount"),
        F.round(F.avg(F.col("a.burst_id")), 2).alias("avg_burst_id"),
        F.grouping_id().alias("gid"),
    )
)
