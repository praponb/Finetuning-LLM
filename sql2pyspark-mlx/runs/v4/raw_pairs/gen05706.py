from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.order_date") > "2025-04-01")
    .select(
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
    )
    .orderBy("opened_date")
)
