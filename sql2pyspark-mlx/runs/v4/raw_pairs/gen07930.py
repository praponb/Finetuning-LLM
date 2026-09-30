from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 1718000) & (F.col("budget") > 2265000))
    .groupBy(
        F.when(F.col("budget") >= 3209000, "high").when(F.col("budget") >= 1276000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.countDistinct("budget").alias("unique_dept_names"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .filter(F.col("unique_dept_names") >= 3)
    .orderBy("budget_bucket", "unique_dept_names")
)
