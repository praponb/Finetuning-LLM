from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy("salary", "id")
w3 = Window.orderBy("hire_date", "id")
w4 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc_nulls_first(), "id")
w5 = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("dept_id", "job_title")

filtered = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-03-08"))) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.concat("level", F.lit("/"), "full_name").alias("level_full_name"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("resolved_level"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_5"),
        F.ntile(10).over(w2).alias("salary_bucket"),
        F.lead("salary", 1).over(w3).alias("next_salary"),
    )
)

base = (
    with_windows
    .filter(F.col("hire_date") >= F.lit("2023-03-05").cast("date"))
    .withColumn("is_remote_label_2", F.when(F.col("is_remote"), "Yes").otherwise("No"))
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("DIRECT")))
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "level_full_name",
        "is_remote_label",
    )
)

with_windows_employees = (
    latest
    .withColumn("running_avg_salary", F.avg("salary").over(w5))
    .withColumn("running_min_salary", F.min("salary").over(w5))
    .withColumn("running_count_salary", F.count("salary").over(w5))
    .withColumn("running_max_salary", F.max("salary").over(w5))
    .withColumn("dept_id_max_salary", F.max("salary").over(w6))
)

result = (
    with_windows_employees
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .groupingSets(
        [["location", "job_title"], ["location"], []],
        F.col("d.location").alias("location"),
        F.col("a.job_title").alias("job_title"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.running_avg_salary")).alias("max_running_avg_salary"),
        F.round(F.avg(F.col("a.running_min_salary")), 2).alias("avg_running_min_salary"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("avg_running_min_salary").desc_nulls_first())
)
