from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.email").alias("employee_email"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.email").alias("manager_email"),
        F.col("m.job_title").alias("manager_job_title"),
        F.initcap(F.trim(F.col("e.email"))).alias("email_title"),
    )
    .orderBy(F.col("employee_email").desc(), "id")
    .limit(3)
)
