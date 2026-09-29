from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.order_date") > "2023-03-26") | F.col("s.order_id").isNotNull())
    .select(
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("s.cost"),
        F.col("s.status"),
        F.floor(F.col("s.cost")).alias("cost_whole"),
    )
    .orderBy(F.col("cost_whole").desc())
)
