from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "channel", "amount", "discount", "shipping_fee")
    .unpivot(
        ["id", "channel"],
        ["amount", "discount", "shipping_fee"],
        "kpi",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
