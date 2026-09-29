from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
w_recent = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-05-26 18:00:00"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.trim("method").alias("clean_method"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

scored = (
    valid
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "method_filled",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.rank().over(w2).alias("amount_rank"),
    )
)

metrics = (
    scored
    .filter(F.col("prev_amount") >= 1580)
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "prev_amount",
        "amount_rank",
        F.avg("prev_amount").over(w_recent).alias("last4_avg_prev_amount"),
        F.count("prev_amount").over(w_recent).alias("last4_count_prev_amount"),
        F.min("prev_amount").over(w_recent).alias("last4_min_prev_amount"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.order_date"))
)

result = (
    joined
    .select(
        "id",
        "order_id",
        "prev_amount",
        "last4_min_prev_amount",
        "last4_avg_prev_amount",
    )
    .unpivot(
        ids=["id", "order_id"],
        values=["prev_amount", "last4_min_prev_amount", "last4_avg_prev_amount"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
