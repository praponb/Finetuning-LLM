from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.region").isin(["West", "North"]) & F.lower(F.col("s.store_name")).like("%ive%"))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.dayofweek(F.col("o.order_date")).alias("order_date_dayofweek"),
    )
    .orderBy(F.col("sqft").desc())
)
