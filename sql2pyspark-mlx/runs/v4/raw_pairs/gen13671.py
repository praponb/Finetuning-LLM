from pyspark.sql import functions as F

base = df_employees.filter(F.lower("full_name").like("%lic%"))

summary = (
    base
    .groupBy(
        "level",
        F.when(F.col("job_title").isin(["Analyst", "Designer"]), "priority").otherwise("other").alias("job_title_group"),
    )
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    summary
    .select("level", "job_title_group", "total_salary")
    .orderBy("job_title_group", "level")
    .limit(20)
)
