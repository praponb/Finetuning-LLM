from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("manager_rows").desc())

result = (
    df_employees
    .groupBy(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"), "dept_id")
    .agg(
        F.percentile_approx("salary", 0.9).alias("salary_p90"),
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
        F.avg("salary").alias("avg_salary"),
        F.count_if(F.col("level") == "L3").alias("l3_events"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_rows"),
    )
    .withColumn("manager_rows_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("quarter_start").asc_nulls_last())
)
