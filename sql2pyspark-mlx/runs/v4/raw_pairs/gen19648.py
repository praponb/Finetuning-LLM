from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("salary").desc())
w_running = Window.partitionBy("manager_id", "dept_id").orderBy("salary", "id")
w4 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-5, -1)

scored = (
    df_employees
    .withColumn("row_seq", F.row_number().over(w_part))
    .withColumn("salary_cume_dist", F.cume_dist().over(w2))
    .withColumn("prev_salary", F.lag("salary", 2).over(w_running))
    .withColumn("salary_avg_5_prior", F.avg("salary").over(w4))
)

result = (
    scored
    .groupBy("level")
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.avg(F.when(F.col("job_title") == "Engineer", F.col("salary"))).alias("engineer_avg_salary"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
    )
    .orderBy(F.col("manager_count").asc_nulls_first())
)
