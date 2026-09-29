from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Kany%"))
    .groupBy("level")
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
    )
    .filter(F.col("pct_designer") >= 45)
    .select("level", "max_salary", "pct_designer")
    .orderBy(F.col("pct_designer").desc())
)
