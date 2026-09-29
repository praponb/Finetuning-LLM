from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("quarter", "hire_date").alias("quarter_start"))
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
    .filter(F.col("distinct_levels") >= 1)
    .orderBy(F.col("distinct_levels").desc_nulls_last())
)
