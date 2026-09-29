from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter((F.col("dept_name") == "Finance") & (F.col("dept_name") == "Engineering"))
)

summary = (
    valid_departments
    .groupBy(
        "dept_name",
        F.when(F.col("dept_name").isin(["Legal", "Sales"]), "tier_1").otherwise("other").alias("dept_name_group"),
    )
    .agg(F.sum("budget").alias("total_budget"))
)

result = (
    summary
    .select("dept_name", "dept_name_group", "total_budget")
    .orderBy("total_budget", "dept_name")
)
