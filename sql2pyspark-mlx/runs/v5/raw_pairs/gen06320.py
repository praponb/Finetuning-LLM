from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc())

ranked = (
    df_employees
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "full_name", "dept_id", "job_title", "is_remote")
)

result = (
    ranked
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_rows"),
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
    )
)
