from pyspark.sql import functions as F

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("amount").isNotNull())
    .select(
        "*",
        F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"),
        F.coalesce("channel", F.lit("NONE")).alias("resolved_channel"),
    )
)

result = (
    valid
    .select("id", "customer_id", "shipping_fee", "amount")
    .unpivot(
        ids=["id", "customer_id"],
        values=["shipping_fee", "amount"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
