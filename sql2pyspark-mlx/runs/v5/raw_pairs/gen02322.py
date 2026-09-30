from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-04-17").cast("date")) & F.col("is_remote"))
    .select(
        "id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.trim("job_title").alias("clean_job_title"),
    )
)

filtered = (
    base
    .filter(F.month("hire_date").isin([2, 10, 12]))
    .select("*", F.when(F.col("level") == "L4", 1).otherwise(0).alias("is_l4"))
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

base_employees = (
    deduped
    .filter((F.col("is_l4") == 54) & F.col("manager_id").isNotNull())
    .withColumn("salary_whole", F.floor("salary"))
)

joined = (
    base_employees
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "src.id",
        "src.manager_id",
        "src.job_title",
        "src.clean_job_title",
        "src.is_l4",
        "src.salary_whole",
        F.col("d.job_title").alias("employee_job_title"),
    )
)

base_employees_2 = (
    joined
    .filter(F.col("manager_id").isNull())
    .select(
        "id",
        "manager_id",
        "is_l4",
        "salary_whole",
        "employee_job_title",
        F.coalesce("is_l4", F.lit(0)).alias("is_l4_filled"),
    )
)

result = (
    base_employees_2
    .alias("s")
    .join(df_employees.alias("p"), F.col("s.manager_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["job_title", "employee_job_title"], ["job_title"], []],
        F.col("p.job_title").alias("job_title"),
        F.col("s.employee_job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("s.salary_whole")), 2).alias("avg_salary_whole"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("num_rows").asc_nulls_last())
)
