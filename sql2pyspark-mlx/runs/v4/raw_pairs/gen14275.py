from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "outer")
    .filter(F.upper(F.trim(F.col("e.email"))) == "FRANK@COMPANY.CO")
    .groupBy(
        F.when(F.col("d.budget") >= 3168000, "high").when(F.col("d.budget") >= 1592000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("d.location").isin(["Chiang Mai", "London"]), "tier_1").otherwise("other").alias("location_group"),
    )
    .agg(F.countDistinct(F.col("e.salary")).alias("unique_depts"))
    .filter(F.col("unique_depts") >= 4)
    .orderBy(F.col("unique_depts").desc())
)
