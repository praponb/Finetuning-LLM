from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter(F.col("order_date") > F.lit("2023-02-24").cast("date"))
    .select(
        "*",
        (F.col("amount") - F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .cube(
        F.col("d.region").alias("region"),
        F.col("d.city").alias("city"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.discount")).alias("min_discount"),
        F.max(F.col("a.amount")).alias("max_amount"),
    )
    .orderBy(F.col("min_discount").asc_nulls_first())
)
