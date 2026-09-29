from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")

ranked = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "full_name", "dept_id", "job_title", "salary", "hire_date", "level")
)

result = (
    ranked
    .groupBy("level")
    .agg(
        F.count_if(F.col("job_title") == "Manager").alias("manager_events"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("job_title") == "Intern", 1)), F.lit(0)), 2).alias("salary_per_intern"),
        F.max("salary").alias("max_salary"),
    )
    .filter(F.col("max_salary") > 0)
)
