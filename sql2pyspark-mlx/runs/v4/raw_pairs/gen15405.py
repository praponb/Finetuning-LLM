from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "PENDING")
    .select("id", "channel", "order_date")
    .intersect(
        df_orders
        .filter(F.col("channel").isin(["partner", "web", "mobile"]))
        .select("id", "channel", "order_date")
    )
)
