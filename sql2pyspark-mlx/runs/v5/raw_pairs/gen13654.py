from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") >= 3400) & (F.col("sale_date") <= "2024-08-01"))
    .groupBy(
        "product_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.count("units").alias("units_count"),
    )
)
