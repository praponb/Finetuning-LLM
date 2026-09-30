from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Finance", "Legal"]))
    .withColumn("location_lower", F.lower("location"))
)

summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 4359000, "high").when(F.col("budget") >= 1491000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.min("budget").alias("min_budget"),
    )
)

result = summary
