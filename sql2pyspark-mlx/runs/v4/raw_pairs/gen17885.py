from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(23, 32))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "product_id",
    )
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") < 10)
    .select("sale_cohort", "product_id", "row_count")
    .orderBy("sale_cohort", "product_id")
    .limit(3)
)
