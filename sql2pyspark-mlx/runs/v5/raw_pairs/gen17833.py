from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("sale_date") < "2025-06-01")

summary = (
    filtered_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "product_id",
    )
    .agg(F.max("revenue").alias("max_revenue"))
)

result = (
    summary
    .filter(F.col("max_revenue") < 1100)
    .select("sale_cohort", "product_id", "max_revenue")
    .orderBy("product_id", "sale_cohort")
    .limit(20)
)
