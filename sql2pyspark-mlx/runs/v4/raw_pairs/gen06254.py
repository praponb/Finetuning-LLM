from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.units").isNull())
    .groupBy(
        F.month(F.col("ss.sale_date")).alias("sale_month"),
        F.when(F.col("ss.units") >= 36, "high").when(F.col("ss.units") >= 12, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .filter(F.col("quantity_range") > 39)
    .orderBy(F.col("quantity_range").desc(), "sale_month", "units_bucket")
    .limit(1)
)
