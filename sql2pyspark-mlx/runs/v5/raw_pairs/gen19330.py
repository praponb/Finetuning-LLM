from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("amount", "id")

filtered = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.upper("method").alias("method_upper"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_filled"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.channel"))
)

scored = (
    enriched
    .select(
        "id",
        "method",
        "amount",
        "method_upper",
        "method_filled",
        "channel",
        F.lag("amount").over(w).alias("prev_amount"),
        F.row_number().over(w).alias("seq_no"),
    )
)

cleaned = (
    scored
    .filter(F.col("method").isNotNull() & F.col("amount").between(420, 970))
    .select(
        "id",
        "method",
        "amount",
        "method_upper",
        "method_filled",
        "channel",
        "prev_amount",
        "seq_no",
        F.lower("method").alias("method_lower"),
        F.round("amount", 0).alias("amount_rounded"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "method",
        "amount",
        "method_filled",
        "channel",
        "prev_amount",
        "seq_no",
        "amount_rounded",
    )
    .orderBy("method")
)
