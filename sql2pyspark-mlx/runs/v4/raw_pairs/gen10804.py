from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.count("units").alias("units_count"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .orderBy(F.col("units_count").desc())
)
