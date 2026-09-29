from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("salary").desc())
w_grp = Window.partitionBy("job_title")
w3 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("salary").desc())
w_part = Window.partitionBy("manager_id")
w5 = Window.partitionBy("job_title").orderBy(F.col("salary_cume_dist").desc_nulls_first())
w6 = Window.partitionBy("employee_job_title")

with_dims = (
    df_employees
    .alias("e")
    .join(df_employees.alias("p2"), F.col("e.manager_id") == F.col("p2.id"), "inner")
    .filter(F.col("p2.level") == "L5")
    .select(
        "e.id",
        "e.full_name",
        "e.manager_id",
        "e.job_title",
        "e.salary",
        "e.hire_date",
        "e.is_remote",
        F.col("p2.email").alias("employee_email"),
        F.col("p2.job_title").alias("employee_job_title"),
    )
)

kept_keys = (
    with_dims
    .select("job_title", "manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("level") == "L1")
        .select("job_title", "manager_id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "*",
        F.cume_dist().over(w).alias("salary_cume_dist"),
        F.avg("salary").over(w_grp).alias("job_title_avg_salary"),
        F.cume_dist().over(w3).alias("salary_cume_dist_2"),
        F.min("salary").over(w_grp).alias("job_title_min_salary"),
        F.avg("salary").over(w_part).alias("manager_id_avg_salary"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "employee_email",
        "employee_job_title",
        "job_title_avg_salary",
        F.cume_dist().over(w5).alias("salary_cume_dist_cume_dist"),
        F.round((F.col("manager_id_avg_salary") / F.sum("manager_id_avg_salary").over(w_part)), 4).alias("manager_id_avg_salary_share"),
        F.round((F.col("salary") / F.sum("salary").over(w6)), 4).alias("salary_share"),
        F.count("manager_id_avg_salary").over(w_grp).alias("job_title_count_manager_id_avg_salary"),
        F.min("manager_id_avg_salary").over(w_grp).alias("job_title_min_manager_id_avg_salary"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "employee_job_title",
        "job_title_avg_salary",
        "salary_cume_dist_cume_dist",
        "salary_share",
        "job_title_count_manager_id_avg_salary",
        "job_title_min_manager_id_avg_salary",
        F.when(F.col("employee_job_title").rlike("^(Designer|Engineer)$"), "EXTENDED").when(F.col("employee_job_title").rlike("^(Analyst|Manager)$"), "GROUP_B").otherwise("GROUP_A").alias("employee_job_title_zone"),
    )
)
