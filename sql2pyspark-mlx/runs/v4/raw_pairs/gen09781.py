from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("brand") == "Globex") & (F.col("supplier_id").isin([14, 17, 32]) | F.col("brand").isin(["Umbrella", "Globex", "Initech"])))
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        F.when(F.col("cost") >= 720, "high").when(F.col("cost") >= 150, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
    )
    .orderBy("launch_cohort")
)
