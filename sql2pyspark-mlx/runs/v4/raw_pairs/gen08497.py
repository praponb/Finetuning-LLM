from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w_hist = Window.partitionBy("job_title", "manager_id").orderBy("salary", "id")
w3 = Window.partitionBy("level", "job_title").orderBy("hire_date", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_employees
    .select(
        "*",
        F.rank().over(w_grp).alias("salary_rank"),
        F.lag("salary", 1).over(w_hist).alias("prev_salary"),
        F.avg("salary").over(w3).alias("salary_avg_3"),
    )
)

result = (
    enriched
    .groupBy("dept_id", "job_title")
    .agg(
        F.max("salary_avg_3").alias("max_salary_avg_3"),
        F.count_if(F.col("level") == "L4").alias("l4_events"),
        F.percentile_approx("salary_rank", [0.25, 0.5, 0.75]).alias("salary_rank_quartiles"),
        F.count(F.when(F.col("level") == "L5", 1)).alias("l5_rows"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)
