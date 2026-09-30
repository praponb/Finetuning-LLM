from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("*").alias("store_sale_count"),
    )
    .filter(F.col("avg_units") > 28)
    .orderBy(F.col("avg_units").desc())
)
