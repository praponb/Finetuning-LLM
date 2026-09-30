from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 2400)
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.max("units").alias("max_units"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .orderBy("sale_cohort")
)
