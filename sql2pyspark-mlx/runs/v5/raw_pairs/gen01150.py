from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget_int").desc())
w_ordered = Window.partitionBy("dept_name").orderBy("budget", "id")
w3 = Window.orderBy("budget_int", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("employee_level").orderBy(F.col("budget_int_rank").desc(), "id")
w_part = Window.partitionBy("employee_level").orderBy("budget", "id")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "*",
        F.col("budget").cast("int").alias("budget_int"),
        F.lower("location").alias("location_lower"),
        F.coalesce("dept_name", F.lit("NONE")).alias("resolved_dept_name"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("budget_int_rank"),
        F.lead("budget_int", 1).over(w_ordered).alias("next_budget_int"),
        F.ntile(10).over(w3).alias("budget_int_bucket"),
    )
)

with_latest = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.budget_int",
        "s.location_lower",
        "s.resolved_dept_name",
        "s.budget_int_rank",
        "s.budget_int_bucket",
        F.col("c.level").alias("employee_level"),
        F.col("c.salary").alias("employee_salary"),
    )
)

latest = (
    with_latest
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

scored = (
    latest
    .select(
        "id",
        "resolved_dept_name",
        "budget_int_bucket",
        "employee_level",
        "employee_salary",
        F.lead("budget_int_bucket").over(w_part).alias("next_budget_int_bucket"),
        F.lag("budget").over(w_part).alias("prev_budget"),
        F.lead("budget").over(w_part).alias("next_budget"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("next_budget_int_bucket") >= 7, "HIGH").otherwise("LOW").alias("next_budget_int_bucket_tier"),
    )
    .agg(
        F.percentile_approx("prev_budget", [0.25, 0.5, 0.75]).alias("prev_budget_quartiles"),
        F.size(F.collect_set("employee_level")).alias("num_employee_levels"),
        F.count(F.when(F.col("employee_level") == "L2", 1)).alias("l2_count"),
        F.count_if(F.col("employee_level") == "L2").alias("l2_events"),
    )
    .filter(F.col("l2_events") >= 1)
)
