from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("level", "dept_name").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("job_title")
w4 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-10-02").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "level",
        F.initcap(F.trim("full_name")).alias("full_name_title"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.budget"))
)

with_dims_employees = (
    with_dims
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.dept_name"),
        F.col("b.location"),
        F.col("b.budget").alias("department_budget"),
    )
)

ranked = (
    with_dims_employees
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    ranked
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.budget").alias("department_budget_2"),
        F.col("p.dept_name").alias("department_dept_name"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.sum("department_budget").over(w2).alias("cumulative_department_budget"),
        F.max("department_budget").over(w3).alias("job_title_max_department_budget"),
        F.avg("department_budget").over(w4).alias("department_budget_avg_2"),
    )
)

with_dims_employees_2 = (
    scored
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .select(
        "src.*",
        F.col("d.dept_name").alias("department_dept_name_2"),
        F.col("d.budget").alias("department_budget_3"),
    )
)

result = (
    with_dims_employees_2
    .groupBy(
        F.when(F.col("department_budget_2") >= 3136000, "HIGH").otherwise("LOW").alias("department_budget_2_tier"),
    )
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.min("budget").alias("min_budget"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
    )
    .filter(F.col("num_dept_names") > 1)
    .orderBy(F.col("department_budget_2_tier").desc())
)
