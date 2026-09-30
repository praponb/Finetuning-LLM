from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("location").orderBy("budget", "id")
w_ordered_2 = Window.partitionBy("dept_name")
w4 = Window.partitionBy("dept_name", "location").orderBy("row_seq", "id")
w5 = Window.partitionBy("location")

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("budget_level", F.when(F.col("budget") >= 4153000, "High").when(F.col("budget") >= 876000, "Medium").otherwise("Low"))
    .withColumn("dept_name_location_key", F.concat_ws("-", "dept_name", "location"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)

with_windows = (
    with_latest
    .select(
        "*",
        F.lead("budget").over(w_ordered).alias("next_budget"),
        F.lag("budget").over(w_ordered).alias("prev_budget"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.min("budget").over(w_ordered_2).alias("dept_name_min_budget"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "dept_name",
        "location",
        "budget_level",
        "employee_hire_date",
        "row_seq",
        "dept_name_min_budget",
        F.lag("budget").over(w4).alias("prev_budget_2"),
        F.lead("prev_budget").over(w4).alias("next_prev_budget"),
        F.row_number().over(w4).alias("row_seq_2"),
    )
)

remaining_keys = (
    enriched
    .select("location")
    .subtract(
        df_departments
        .filter(F.col("location").isin(["Tokyo", "Berlin"]))
        .select("location")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

eligible = (
    kept_rows
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.is_remote")), "left_anti")
)

enriched_departments = (
    eligible
    .select(
        "*",
        F.stddev_samp("dept_name_min_budget").over(w5).alias("location_stddev_samp_dept_name_min_budget"),
        F.min("next_prev_budget").over(w_ordered_2).alias("dept_name_min_next_prev_budget"),
        F.round((F.col("row_seq_2") / F.sum("row_seq_2").over(w5)), 4).alias("row_seq_2_share"),
    )
)

result = (
    enriched_departments
    .select(
        "id",
        "dept_name",
        "budget_level",
        "employee_hire_date",
        "dept_name_min_budget",
        "prev_budget_2",
        "next_prev_budget",
        "row_seq_2",
        "location_stddev_samp_dept_name_min_budget",
        F.when(F.col("dept_name").rlike("^(Finance|Sales)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("dept_name_zone"),
    )
    .orderBy(F.col("dept_name_zone").desc_nulls_last())
)
