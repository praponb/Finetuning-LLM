from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("status")

cleaned = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-05-29").cast("date"))
    .select(
        "*",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

remaining_keys = (
    cleaned
    .select("id", "status")
    .subtract(
        df_payments
        .filter(F.col("paid_at") >= "2023-03-01")
        .select("id", "status")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.max("amount_int").over(w2).alias("status_max_amount_int"),
    )
)

cleaned_payments = (
    scored
    .filter(F.col("paid_at") >= F.lit("2023-05-10").cast("date"))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "amount_int",
        "resolved_method",
        "amount_share",
        "status_max_amount_int",
        F.date_trunc("year", "paid_at").alias("paid_at_period"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

enriched = (
    cleaned_payments
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.channel"), F.col("d.discount"))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        "amount_int",
        "amount_share",
        "paid_at_period",
        "method_filled",
        "channel",
    )
    .orderBy("status")
)
