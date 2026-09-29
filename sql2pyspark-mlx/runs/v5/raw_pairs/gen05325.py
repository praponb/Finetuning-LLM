from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Taylo%") & F.col("level").isin(["L5", "L1", "L3"]))
    .groupBy("job_title", F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
        F.max("hire_date").alias("latest_hire_date"),
    )
    .orderBy(F.col("pct_l4").desc(), "job_title", "hire_month")
    .limit(20)
)
