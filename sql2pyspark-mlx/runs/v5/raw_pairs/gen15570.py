from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("location").isin(["Tokyo", "Berlin"]), "tier_1").otherwise("other").alias("location_group"),
        "dept_name",
    )
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.min("budget").alias("min_budget"),
        F.countDistinct("budget").alias("unique_dept_names"),
    )
    .filter(F.col("unique_dept_names") >= 4)
    .drop("unique_dept_names")
    .orderBy(F.col("berlin_count").desc())
)
