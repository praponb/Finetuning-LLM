from pyspark.sql import functions as F

src = (
    df_employees
    .filter(~((F.col("salary") < 78000) & F.col("is_remote") & ~F.col("full_name").like("%Jac%")))
)

summary = (
    src
    .groupBy("job_title")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    summary
    .select("job_title", "pct_designer", "min_salary")
    .orderBy("pct_designer")
)
