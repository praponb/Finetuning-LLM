from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("is_remote"))
    .withColumn("hire_date_month", F.month("hire_date"))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("salary") >= 162000, "HIGH").otherwise("LOW").alias("salary_tier"),
    )
    .agg(
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles_2"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("salary_p90") < 0)
)
