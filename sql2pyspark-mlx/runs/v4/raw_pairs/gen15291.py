from pyspark.sql import functions as F

enriched = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.status"), F.col("o.amount"))
)

result = (
    enriched
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("amount")).otherwise(0)).alias("returned_amount"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("amount_per_cancelled"),
        F.count("*").alias("cnt"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") < 0)
)
