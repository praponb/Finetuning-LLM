from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("stock_qty") != 124) & ~F.col("brand").isin(["Stark", "Globex", "Initech"]) & F.col("supplier_id").isin([6, 17, 40]))
    .groupBy(
        "category",
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
    )
    .orderBy("home_count", "stark_count")
)
