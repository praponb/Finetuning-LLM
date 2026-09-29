from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "category",
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .filter(F.col("cost_range") < 53)
)
