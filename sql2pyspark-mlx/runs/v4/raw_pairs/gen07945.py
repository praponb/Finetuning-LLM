from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4, 5]))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        F.year("sale_date").alias("sale_year"),
    )
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .orderBy(F.col("min_sale_date").desc())
)
