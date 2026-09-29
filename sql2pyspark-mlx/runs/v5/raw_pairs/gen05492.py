from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        F.length("email").alias("email_length"),
    )
)

joined = (
    filtered
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name") == "Engineering")
    .select("a.*", F.col("d.location"), F.col("d.dept_name"), F.col("d.budget"))
)

joined_employees = (
    joined
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.level").alias("employee_level"),
        F.col("p.job_title"),
        F.col("p.full_name").alias("employee_full_name"),
    )
)

cleaned = (
    joined_employees
    .filter(F.col("dept_id").isin([11, 13, 20, 28]))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "level",
        "dept_name",
        "employee_level",
        "job_title",
        "employee_full_name",
        F.when(F.col("email_length") >= 88, "High").when(F.col("email_length") >= 29, "Medium").otherwise("Low").alias("email_length_band"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

with_windows = (
    cleaned
    .withColumn("last3_sum_salary", F.sum("salary").over(w))
    .withColumn("last3_avg_salary", F.avg("salary").over(w))
    .withColumn("last3_max_salary", F.max("salary").over(w))
)

matched_keys = (
    with_windows
    .select("id", "dept_id")
    .subtract(
        df_employees
        .filter(F.col("dept_id") == 13)
        .select("id", "dept_id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

stats = (
    reconciled
    .groupBy("dept_name")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(F.sum("last3_avg_salary") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("last3_avg_salary_per_engineering"),
        F.sum(F.when(F.col("employee_level") == "L5", F.col("last3_max_salary")).otherwise(0)).alias("l5_last3_max_salary"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .filter(F.col("l5_last3_max_salary") > 0)
)

result = (
    stats
    .select(
        "dept_name",
        "level_list",
        "last3_avg_salary_per_engineering",
        "l5_last3_max_salary",
        "num_levels",
        F.when(F.col("dept_name").rlike("^(HR|Marketing)$"), "PRIORITY").when(F.col("dept_name").rlike("^(Engineering|Sales)$"), "GROUP_A").otherwise("GROUP_B").alias("dept_name_zone"),
    )
    .orderBy("last3_avg_salary_per_engineering", "dept_name")
    .limit(10)
)
