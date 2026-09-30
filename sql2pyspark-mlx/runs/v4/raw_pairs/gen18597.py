from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id")
w2 = Window.partitionBy("dept_id", "manager_id").orderBy("salary", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("job_title")
w4 = Window.partitionBy("dept_id")
w5 = Window.orderBy("salary")
w6 = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w7 = Window.orderBy("dept_id_avg_manager_id_min_salary")
w_ordered = Window.partitionBy("dept_id").orderBy("dept_id_avg_manager_id_min_salary")

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2024-03-23").cast("date")) & (F.length("full_name") >= 13))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.trim("job_title").alias("clean_job_title"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_filled"),
    )
)

metrics = (
    base
    .withColumn("manager_id_min_salary", F.min("salary").over(w))
    .withColumn("salary_avg_3", F.avg("salary").over(w2))
)

kept_keys = (
    metrics
    .select("manager_id")
    .intersect(
        df_employees
        .filter(F.col("dept_id") == 5)
        .select("manager_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.round((F.col("manager_id_min_salary") / F.sum("manager_id_min_salary").over(w3)), 4).alias("manager_id_min_salary_share"),
        F.avg("manager_id_min_salary").over(w4).alias("dept_id_avg_manager_id_min_salary"),
        F.percent_rank().over(w5).alias("salary_percent_rank"),
    )
)

result = (
    scored
    .withColumn("dept_id_count_salary_avg_3", F.count("salary_avg_3").over(w4))
    .withColumn("salary_rank", F.rank().over(w6))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("dept_id_avg_manager_id_min_salary_dense_rank", F.dense_rank().over(w7))
    .withColumn("dept_id_avg_manager_id_min_salary_percent_rank", F.percent_rank().over(w_ordered))
)
