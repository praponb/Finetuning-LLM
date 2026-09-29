from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name") != "Marketing"))
    .select("*", F.round((F.col("budget") * 0.9), 2).alias("adjusted_budget"))
)

filtered_departments = (
    filtered
    .filter(F.col("adjusted_budget").between(31, 67))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "adjusted_budget",
        F.when(F.col("dept_name").isin(["HR", "Finance"]), "Core").otherwise("Other").alias("dept_name_group"),
        (F.col("adjusted_budget") + F.col("budget")).alias("adjusted_budget_plus_budget"),
        F.coalesce("dept_name", F.lit("N/A")).alias("resolved_dept_name"),
    )
)

with_last = (
    filtered_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.adjusted_budget",
        "s.adjusted_budget_plus_budget",
        "s.resolved_dept_name",
        F.col("c.salary").alias("employee_salary"),
        F.col("c.level"),
    )
)

latest_match = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.level", F.col("c.hire_date"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title") != "Manager"), "left_semi")
)

matched_keys = (
    kept
    .select("id")
    .intersectAll(
        df_departments
        .filter(F.col("dept_name") == "Marketing")
        .select("id")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("level")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count_if(F.col("level") == "L3").alias("l3_events"),
        F.count_if(F.col("level") == "L4").alias("l4_events"),
    )
    .orderBy(F.col("level").desc_nulls_last())
)
