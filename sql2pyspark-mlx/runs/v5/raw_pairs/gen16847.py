from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") > "2025-02-01")
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        "brand",
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", F.col("stock_qty")).otherwise(0)).alias("initech_stock_qty"),
    )
    .orderBy(F.col("launch_cohort").desc(), "brand")
    .limit(3)
)
