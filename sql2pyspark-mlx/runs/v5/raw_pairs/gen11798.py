from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") < 125000) & F.col("salary").between(61000, 75000))
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0))) / F.count("*")), 1).alias("pct_analyst"),
    )
    .orderBy(F.col("pct_analyst").desc(), "unique_levels")
)
