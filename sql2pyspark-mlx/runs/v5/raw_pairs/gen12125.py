from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.countDistinct("units").alias("unique_stores"),
        F.avg("revenue").alias("avg_revenue"),
    )
    .orderBy(F.col("unique_stores").desc(), "sale_cohort")
    .limit(5)
)
