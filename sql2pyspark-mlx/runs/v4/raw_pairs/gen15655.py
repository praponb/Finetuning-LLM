from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("employee_salary").desc_nulls_first())
w2 = Window.partitionBy("job_title", "dept_id").orderBy("salary")
w3 = Window.orderBy(F.col("employee_salary").desc_nulls_first())
w4 = Window.partitionBy("job_title")
w5 = Window.partitionBy("manager_id", "dept_id").orderBy(F.col("employee_salary_cume_dist").desc_nulls_first())
w6 = Window.partitionBy("dept_id")
w7 = Window.partitionBy("employee_job_title").orderBy(F.col("salary_percent_rank").desc())
w8 = Window.partitionBy("job_title", "level")
w9 = Window.orderBy(F.col("employee_salary_percent_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("employee_salary").desc_nulls_last())
w11 = Window.partitionBy("employee_job_title").orderBy("employee_salary_percent_rank_cume_dist")
w12 = Window.orderBy("employee_salary_cume_dist_dense_rank")
w13 = Window.orderBy("employee_salary_share")
w14 = Window.orderBy(F.col("employee_salary_percent_rank").asc_nulls_last())
w15 = Window.orderBy("employee_salary")

remaining_keys = (
    df_employees
    .select("manager_id")
    .subtract(
        df_employees
        .filter(F.col("salary") < 112000)
        .select("manager_id")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.job_title").alias("employee_job_title"),
        F.col("b.hire_date").alias("employee_hire_date"),
        F.col("b.salary").alias("employee_salary"),
    )
)

enriched = (
    with_dims
    .withColumn("employee_salary_percent_rank", F.percent_rank().over(w))
    .withColumn("salary_percent_rank", F.percent_rank().over(w2))
    .withColumn("employee_salary_cume_dist", F.cume_dist().over(w3))
    .withColumn("job_title_sum_salary", F.sum("salary").over(w4))
)

enriched_employees = (
    enriched
    .withColumn("employee_salary_cume_dist_dense_rank", F.dense_rank().over(w5))
    .withColumn("dept_id_max_job_title_sum_salary", F.max("job_title_sum_salary").over(w6))
    .withColumn("salary_percent_rank_percent_rank", F.percent_rank().over(w7))
    .withColumn("employee_salary_share", F.round((F.col("employee_salary") / F.sum("employee_salary").over(w8)), 4))
)

joined = (
    enriched_employees
    .alias("s")
    .join(df_employees.alias("p"), F.col("s.manager_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.email").alias("employee_email"),
        F.col("p.level").alias("employee_level"),
    )
)

with_windows = (
    joined
    .withColumn("employee_salary_percent_rank_cume_dist", F.cume_dist().over(w9))
    .withColumn("employee_salary_rank", F.rank().over(w10))
)

result = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w11).alias("employee_salary_percent_rank_cume_dist_percent_rank"),
        F.cume_dist().over(w12).alias("employee_salary_cume_dist_dense_rank_cume_dist"),
        F.percent_rank().over(w13).alias("employee_salary_share_percent_rank"),
        F.percent_rank().over(w14).alias("employee_salary_percent_rank_percent_rank"),
        F.percent_rank().over(w15).alias("employee_salary_percent_rank_2"),
    )
)
