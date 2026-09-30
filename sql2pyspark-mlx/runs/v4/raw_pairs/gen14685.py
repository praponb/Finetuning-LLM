from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Intern", "Engineer"]))
    .select(
        "hire_date",
        "is_remote",
        "email",
        F.concat("job_title", F.lit(" "), "full_name").alias("job_title_full_name"),
    )
)
