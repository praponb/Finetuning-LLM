from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "COMPLETED")
    .select("id", "channel", "order_date")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee") >= 13)
        .select("id", "channel", "order_date")
    )
)
