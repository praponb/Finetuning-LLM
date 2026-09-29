from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.revenue") <= 3300)
    .groupBy(
        F.col("s.region"),
        F.when(F.year(F.col("s.opened_date")) < 2024, "before_2024").when(F.year(F.col("s.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.count(F.col("ss.units")).alias("units_count"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
    .filter(F.col("units_count") > 4)
    .orderBy("region")
)
