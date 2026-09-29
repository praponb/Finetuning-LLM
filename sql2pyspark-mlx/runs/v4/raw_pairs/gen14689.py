from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("running_hire_date_month").desc(), "id")
w4 = Window.partitionBy("dept_id").orderBy(F.col("cumulative_hire_date_month").desc(), "id")
w5 = Window.orderBy("cumulative_hire_date_month", "id")
w6 = Window.orderBy("cumulative_hire_date_month", "id").rowsBetween(-3, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.avg("salary").alias("avg_employee_salary"))

base = (
    df_employees
    .filter(F.length("full_name") >= 14)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        F.month("hire_date").alias("hire_date_month"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_clean"),
    )
)

metrics = (
    base
    .withColumn("cumulative_hire_date_month", F.sum("hire_date_month").over(w))
    .withColumn("running_hire_date_month", F.sum("hire_date_month").over(w2))
)

with_dims = (
    metrics
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.location"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("seq_no_2"),
        F.row_number().over(w5).alias("row_seq"),
        F.avg("cumulative_hire_date_month").over(w6).alias("cumulative_hire_date_month_avg_3"),
    )
)

kept_keys = (
    with_windows
    .select("level", "manager_id")
    .subtract(
        df_employees
        .filter(F.col("job_title") == "Manager")
        .select("level", "manager_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("location").isNotNull())
    .select(
        "id",
        "email",
        "manager_id",
        "hire_date",
        "is_remote",
        "level",
        "hire_date_month",
        "job_title_clean",
        "cumulative_hire_date_month",
        "location",
        "seq_no_2",
        "row_seq",
        "cumulative_hire_date_month_avg_3",
        F.length("level").alias("level_length"),
        F.round("row_seq", 2).alias("row_seq_rounded"),
    )
)

by_type = (
    valid
    .groupBy("manager_id")
    .pivot("level", ["L5", "L2", "L1"])
    .agg(
        F.avg("cumulative_hire_date_month").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
)

result = (
    by_type
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "manager_id",
        "L5_amount",
        "L2_amount",
        "L2_item_count",
        "L1_item_count",
        F.col("avg_employee_salary"),
    )
)
