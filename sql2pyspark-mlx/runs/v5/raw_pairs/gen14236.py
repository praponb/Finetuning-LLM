from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("full_name").like("%ANY%"))
    .groupBy("level", "is_remote")
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
    )
    .filter(F.col("pct_manager") < 41)
    .select("level", "is_remote", "max_salary", "pct_manager")
    .orderBy(F.col("max_salary").desc())
)
