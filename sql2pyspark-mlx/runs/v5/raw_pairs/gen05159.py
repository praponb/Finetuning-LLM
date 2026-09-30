from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy(F.col("amount").asc_nulls_last())
w3 = Window.partitionBy("order_status", "method")
w4 = Window.partitionBy("order_id").orderBy(F.col("amount_adjusted").desc())
w5 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("paid_at") < "2023-12-01")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.round((F.col("amount") * 1.07), 2).alias("amount_adjusted"),
    )
)

kept_keys = (
    valid
    .select("order_id")
    .subtract(
        df_payments
        .filter(F.col("method").isNotNull())
        .select("order_id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .filter(F.col("p.status").isin(["SHIPPED", "RETURNED", "CANCELLED"]))
    .select(
        "src.*",
        F.col("p.status").alias("order_status"),
        F.col("p.channel"),
        F.col("p.order_date"),
    )
)

scored = (
    joined
    .select(
        "order_status",
        "channel",
        F.max("amount_adjusted").over(w).alias("status_max_amount_adjusted"),
        F.cume_dist().over(w2).alias("amount_cume_dist"),
        F.max("amount_adjusted").over(w3).alias("order_status_max_amount_adjusted"),
        F.percent_rank().over(w4).alias("amount_adjusted_percent_rank"),
        F.avg("amount").over(w5).alias("amount_avg_30d"),
    )
)

result = (
    scored
    .select(
        "order_status",
        "amount_cume_dist",
        "order_status_max_amount_adjusted",
        "amount_adjusted_percent_rank",
        "amount_avg_30d",
    )
)
