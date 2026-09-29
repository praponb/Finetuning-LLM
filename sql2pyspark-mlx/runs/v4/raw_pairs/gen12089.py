from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id", "is_remote")
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.max("hire_date").alias("latest_hire_date"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_designer"),
    )
    .orderBy("latest_hire_date")
)
