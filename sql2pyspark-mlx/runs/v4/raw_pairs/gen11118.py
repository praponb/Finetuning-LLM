from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2025-04-01")
    .groupBy(
        "store_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.count("units").alias("units_count"))
    .filter(F.col("units_count") > 5)
    .orderBy("sale_cohort")
)
