from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("job_title").orderBy("salary", "id")
w_grp_2 = Window.partitionBy("dept_id")
w3 = Window.partitionBy("manager_id").orderBy("hire_date", "id")

enriched = (
    df_employees
    .withColumn("salary_bucket", F.ntile(4).over(w_grp))
    .withColumn("dept_id_min_salary", F.min("salary").over(w_grp_2))
    .withColumn("prev_salary", F.lag("salary", 1).over(w3))
)

result = (
    enriched
    .groupBy(
        F.date_trunc("year", "hire_date").alias("year_start"),
        "dept_id",
        "manager_id",
    )
    .agg(
        F.countDistinct("job_title").alias("distinct_job_titles"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .orderBy(F.col("num_levels").desc_nulls_last())
)
