from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "id",
        "dept_id",
        "salary",
        "full_name",
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
)
