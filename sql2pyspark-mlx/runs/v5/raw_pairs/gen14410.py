from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-06-03").cast("date")) & F.col("channel").isNotNull())
    .withColumn("channel_prefix", F.substring("channel", 1, 4))
    .withColumn("days_since_order_date", F.datediff(F.current_date(), "order_date"))
)

result = (
    filtered
    .select("id", "status", "discount", "shipping_fee", "amount")
    .unpivot(
        ids=["id", "status"],
        values=["discount", "shipping_fee", "amount"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
