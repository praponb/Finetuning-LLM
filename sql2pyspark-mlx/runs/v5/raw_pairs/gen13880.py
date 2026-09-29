from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("running_min_salary").desc(), "id")
w3 = Window.partitionBy("manager_id").orderBy(F.col("running_min_salary").desc(), "id")
w4 = Window.partitionBy("location", "manager_id").orderBy(F.col("hire_date").desc(), "id")

scored = (
    df_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        F.avg("salary").over(w_hist).alias("running_avg_salary"),
        F.min("salary").over(w_hist).alias("running_min_salary"),
        F.sum("salary").over(w_hist).alias("running_sum_salary"),
    )
)

valid = (
    scored
    .filter(F.col("manager_id").isNotNull() & F.col("manager_id").isin([13, 21]))
    .withColumn("least_running_avg_salary_running_min_salary", F.least("running_avg_salary", "running_min_salary"))
)

enriched = (
    valid
    .filter(F.col("running_sum_salary") > 378000)
    .select(
        "*",
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)

enriched_employees = (
    enriched
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.dept_id",
        "s.manager_id",
        "s.salary",
        "s.hire_date",
        "s.is_remote",
        F.col("b.dept_name"),
        F.col("b.location"),
        F.col("b.budget"),
    )
)

joined = (
    enriched_employees
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.location").alias("department_location"),
        F.col("b.dept_name").alias("department_dept_name"),
        F.col("b.budget").alias("department_budget"),
    )
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    deduped
    .select("manager_id", "dept_id")
    .intersectAll(
        df_employees
        .filter(F.col("manager_id").isNotNull())
        .select("manager_id", "dept_id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("manager_id")
    .agg(
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.percentile_approx("department_budget", [0.25, 0.5, 0.75]).alias("department_budget_quartiles"),
    )
    .orderBy(F.col("manager_id").desc_nulls_last())
)
