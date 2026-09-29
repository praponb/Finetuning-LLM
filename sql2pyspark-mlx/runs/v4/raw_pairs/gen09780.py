from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(
        F.when(F.col("ss.units") >= 34, "high").when(F.col("ss.units") >= 19, "mid").otherwise("low").alias("units_bucket"),
        F.when(F.year(F.col("ss.sale_date")) < 2024, "before_2024").when(F.year(F.col("ss.sale_date")) == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy("avg_quantity")
)
