from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(
        F.when(F.col("d.location").isin(["London", "Tokyo"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(F.avg(F.col("e.salary")).alias("avg_salary"))
    .orderBy("location_group")
)
