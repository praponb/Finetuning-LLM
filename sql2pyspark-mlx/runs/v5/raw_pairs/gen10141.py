from pyspark.sql import functions as F

cleaned = (
    df_orders
    .filter(F.col("customer_id").isNotNull() & (F.col("amount") < 960))
    .withColumn("discount_whole", F.ceil("discount"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .select("id", "channel", "amount", "shipping_fee")
    .unpivot(
        ids=["id", "channel"],
        values=["amount", "shipping_fee"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
