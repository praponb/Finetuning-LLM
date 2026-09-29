from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("level", "manager_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("running_avg_salary_int", "id")
w5 = Window.partitionBy("dept_id").orderBy(F.col("running_max_salary_int").desc(), "id")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-07-08").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "*",
        F.col("salary").cast("int").alias("salary_int"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .withColumn("running_avg_salary_int", F.avg("salary_int").over(w2))
    .withColumn("running_max_salary_int", F.max("salary_int").over(w2))
    .withColumn("running_min_salary_int", F.min("salary_int").over(w2))
)

enriched = (
    metrics
    .select(
        "id",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "salary_int",
        "job_title_clean",
        "running_max_salary_int",
        F.last("running_max_salary_int", ignorenulls=True).over(w3).alias("last_known_running_max_salary_int"),
        F.lead("running_avg_salary_int", 1).over(w4).alias("next_running_avg_salary_int"),
    )
)

enriched_employees = (
    enriched
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .filter(F.col("b.budget").between(1433000, 3318000))
    .select(
        "e.id",
        "e.dept_id",
        "e.salary",
        "e.hire_date",
        "e.is_remote",
        "e.job_title_clean",
        "e.running_max_salary_int",
        "e.next_running_avg_salary_int",
        F.col("b.budget"),
    )
)

deduped = (
    enriched_employees
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_id", "salary", "hire_date", "is_remote", "job_title_clean")
)

remaining_keys = (
    deduped
    .select("id", "dept_id")
    .intersectAll(
        df_employees
        .filter(F.col("level") == "L5")
        .select("id", "dept_id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .rollup(F.col("p.dept_name").alias("dept_name"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.salary")).alias("min_salary"),
    )
)
