from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("manager_id", "level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("dept_id", "manager_id").orderBy("salary", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("manager_id")
w5 = Window.partitionBy("level", "dept_id").orderBy("salary_avg_5", "id")
w6 = Window.orderBy("salary_avg_5", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("dept_id", "level").orderBy(F.col("seq_no").asc_nulls_first())
w_hist_2 = Window.orderBy("first_known_is_remote_flag_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("is_remote_flag", "id")
w10 = Window.partitionBy("level").orderBy("salary_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2024-01-01"))) & F.col("level").isNotNull() & (F.col("job_title") != "Engineer"))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.when(F.col("salary") >= 120000, "High").when(F.col("salary") >= 86000, "Medium").otherwise("Low").alias("salary_level"),
    )
)

scored = (
    base
    .withColumn("salary_avg_6", F.avg("salary").over(w))
    .withColumn("first_known_is_remote_flag", F.first("is_remote_flag", ignorenulls=True).over(w_recent))
    .withColumn("salary_avg_5", F.avg("salary").over(w_running))
)

metrics = (
    scored
    .select(
        "*",
        F.avg("salary_avg_6").over(w4).alias("manager_id_avg_salary_avg_6"),
        F.row_number().over(w5).alias("seq_no"),
        F.avg("first_known_is_remote_flag").over(w6).alias("first_known_is_remote_flag_avg_5"),
    )
)

filtered = (
    metrics
    .filter((F.col("hire_date") >= F.lit("2023-08-05").cast("date")) & F.col("level").isNotNull())
    .select(
        "*",
        F.when(F.col("salary") >= 139000, "High").when(F.col("salary") >= 86000, "Medium").otherwise("Low").alias("salary_level_2"),
    )
)

scored_employees = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_hist).alias("seq_no_dense_rank"),
        F.sum("salary_avg_6").over(w_hist_2).alias("running_salary_avg_6"),
        F.ntile(5).over(w9).alias("is_remote_flag_bucket"),
        F.sum("first_known_is_remote_flag").over(w10).alias("cumulative_first_known_is_remote_flag"),
    )
)

matched_keys = (
    scored_employees
    .select("id", "manager_id")
    .intersectAll(
        df_employees
        .filter(F.col("hire_date").between("2023-10-24", "2025-06-01"))
        .select("id", "manager_id")
    )
)

kept_rows = (
    scored_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "full_name",
        "dept_id",
        "salary",
        "hire_date",
        "salary_avg_6",
        "first_known_is_remote_flag",
        "salary_avg_5",
        "manager_id_avg_salary_avg_6",
        F.when(F.col("full_name").rlike("^(Alice Wong|Ivy Chen)$"), "CORE").when(F.col("full_name").rlike("^(bob smith|  Carol Diaz)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("full_name_zone"),
    )
    .orderBy(F.col("salary_avg_5").desc_nulls_last(), F.col("dept_id").desc())
)
