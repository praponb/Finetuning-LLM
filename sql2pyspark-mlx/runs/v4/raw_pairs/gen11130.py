from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "job_title",
        "full_name",
        "manager_id",
        F.initcap(F.trim("email")).alias("email_title"),
    )
)
