from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.city").isin(["New York", "Bangkok"]), "core").otherwise("other").alias("city_group"),
        F.col("s.city"),
    )
    .agg(
        F.sum(F.col("oi.quantity")).alias("total_quantity"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
    )
    .filter(F.col("total_quantity") > 15)
    .orderBy(F.col("avg_unit_price").desc())
)
