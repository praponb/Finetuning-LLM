from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Director", "Designer"]) & (F.col("dept_id").isin([14, 16]) | ~F.col("is_remote")))
    .select("id", "dept_id", F.substring("email", 1, 4).alias("email_prefix"))
)
