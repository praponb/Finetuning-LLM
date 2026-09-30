from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "inner")
    .filter(F.month(F.col("stores.opened_date")).isin([4, 6, 11]))
    .groupBy(
        F.when(F.year(F.col("stores.opened_date")) < 2024, "before_2024").when(F.year(F.col("stores.opened_date")) == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(F.max(F.col("store_sales.units")).alias("max_units"))
    .orderBy(F.col("max_units").desc())
)
