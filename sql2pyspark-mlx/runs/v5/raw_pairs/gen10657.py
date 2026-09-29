from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.revenue").isNull())
    .groupBy(
        F.when(F.year(F.col("s.opened_date")) < 2024, "before_2024").when(F.year(F.col("s.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"))
    .filter(F.col("avg_units") > 40)
)
