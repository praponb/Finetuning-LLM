from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("hire_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("job_title")
w4 = Window.partitionBy("manager_id")
w5 = Window.partitionBy("dept_id").orderBy("budget", "id")
w6 = Window.partitionBy("dept_name", "manager_id").orderBy("row_num", "id")

filtered = (
    df_employees
    .filter(~F.col("job_title").eqNullSafe(F.lit("Engineer")))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.substring("job_title", 1, 3).alias("job_title_prefix"),
    )
)

enriched = (
    filtered
    .alias("s")
    .join(df_departments.alias("b"), F.col("s.dept_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.dept_id",
        "s.manager_id",
        "s.job_title",
        "s.hire_date",
        "s.is_remote",
        "s.job_title_prefix",
        F.col("b.dept_name"),
        F.col("b.budget"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

enriched_employees = (
    latest
    .select(
        "id",
        "manager_id",
        "job_title",
        "hire_date",
        "dept_name",
        "row_num",
        F.round((F.col("budget") / F.sum("budget").over(w3)), 4).alias("budget_share"),
        F.round((F.col("budget") / F.sum("budget").over(w4)), 4).alias("budget_share_2"),
        F.row_number().over(w5).alias("row_seq"),
        F.lag("budget", 1).over(w6).alias("prev_budget"),
    )
)

daily = (
    enriched_employees
    .groupBy(
        "manager_id",
        F.when(F.col("budget_share") >= 0.8, "LARGE").otherwise("SMALL").alias("budget_share_tier"),
    )
    .agg(
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("row_seq_per_engineering"),
        F.count_if(F.col("dept_name") == "Legal").alias("legal_events"),
        F.max("budget_share_2").alias("max_budget_share_2"),
        F.percentile_approx("budget_share_2", [0.25, 0.5, 0.75]).alias("budget_share_2_quartiles"),
        F.count(F.when(F.col("job_title") == "Designer", 1)).alias("designer_count"),
    )
)

result = (
    daily
    .select(
        "manager_id",
        "budget_share_tier",
        "distinct_dept_names",
        "row_seq_per_engineering",
        "legal_events",
        "max_budget_share_2",
        "budget_share_2_quartiles",
        "designer_count",
    )
    .orderBy(F.col("distinct_dept_names").desc())
)
