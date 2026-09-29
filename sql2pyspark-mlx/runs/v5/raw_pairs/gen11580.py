from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount_rounded", "id")
w_hist = Window.partitionBy("order_id").orderBy("amount_rounded", "id").rowsBetween(-2, -1)

filtered = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-10-20 17:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "*",
        F.round("amount", 2).alias("amount_rounded"),
        F.date_trunc("week", "paid_at").alias("paid_at_period"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

result = (
    filtered
    .filter(F.col("order_id") == 24)
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "amount_rounded",
        "paid_at_period",
        "resolved_method",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.avg("amount_rounded").over(w_hist).alias("amount_rounded_avg_2_prior"),
    )
)
