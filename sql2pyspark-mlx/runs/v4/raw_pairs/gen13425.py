from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "channel", "discount", "shipping_fee", "amount")
    .unpivot(
        ["id", "channel"],
        ["discount", "shipping_fee", "amount"],
        "measure",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
