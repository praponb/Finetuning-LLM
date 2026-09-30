from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("level", "job_title").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("dept_id", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("salary_rank_2_tier").orderBy(F.col("num_job_titles").desc())

cleaned = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-03-18").cast("date"))
    .select("*", F.substring("full_name", 1, 3).alias("full_name_prefix"))
)

valid = (
    cleaned
    .filter(F.col("hire_date") >= F.lit("2023-04-20").cast("date"))
    .select("*", F.upper(F.trim("email")).alias("email_normalized"))
)

metrics = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("salary_rank"),
        F.rank().over(w2).alias("salary_rank_2"),
        F.sum("salary").over(w3).alias("running_salary"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.budget"), F.col("p.location"), F.col("p.dept_name"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("salary_rank_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("salary_rank_2_tier"),
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Director", F.col("budget")).otherwise(0)).alias("director_budget"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.size(F.collect_set("job_title")).alias("num_job_titles"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("running_salary"))).alias("legal_total_running_salary"),
        F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_rows"),
    )
    .filter(F.col("director_budget") > 0)
    .withColumn("num_job_titles_rank", F.rank().over(w4))
)
