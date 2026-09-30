from pyspark.sql import functions as F

filtered_employees = df_employees.filter(F.col("manager_id").isNull())

summary = (
    filtered_employees
    .groupBy("level")
    .agg(
        (F.max("salary") - F.min("salary")).alias("salary_range"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    summary
    .filter(F.col("salary_range") < 53)
    .select("level", "max_salary")
    .orderBy("max_salary")
)
