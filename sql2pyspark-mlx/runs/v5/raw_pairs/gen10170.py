from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("greatest_salary_latest_rank").desc_nulls_first(), "id")
w3 = Window.orderBy(F.col("salary").desc())
w4 = Window.partitionBy("manager_id")
w_hist = Window.orderBy("salary_dense_rank", "id")

base = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-08-12").cast("date"))
    .select(
        "*",
        F.when(F.col("job_title").isin(["Designer", "Analyst"]), "Tier 1").otherwise("Other").alias("job_title_group"),
        F.coalesce("level", F.lit("NONE")).alias("level_filled"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    latest
    .filter(F.col("manager_id") == 9)
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
        "level",
        "job_title_group",
        "level_filled",
        "latest_rank",
        F.greatest("salary", "latest_rank").alias("greatest_salary_latest_rank"),
        F.when(F.col("level").isin(["L1", "L4"]), "Tier 1").otherwise("Other").alias("level_group"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_filled"),
    )
)

latest_employees = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "latest_rank",
        "greatest_salary_latest_rank",
        "level_group",
    )
)

enriched = (
    latest_employees
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.email"))
)

joined = (
    enriched
    .alias("src")
    .join(df_employees.alias("e"), F.col("src.manager_id") == F.col("e.id"), "left")
    .select(
        "src.id",
        "src.manager_id",
        "src.latest_rank",
        "src.level_group",
        "src.email",
        F.col("e.hire_date"),
        F.col("e.salary"),
    )
)

scored = (
    joined
    .filter(F.col("latest_rank") <= 1)
    .select(
        "id",
        "latest_rank",
        "level_group",
        "email",
        "hire_date",
        F.dense_rank().over(w3).alias("salary_dense_rank"),
        F.round((F.col("salary") / F.sum("salary").over(w4)), 4).alias("salary_share"),
    )
)

result = (
    scored
    .select(
        "id",
        "latest_rank",
        "level_group",
        "hire_date",
        "salary_share",
        F.lead("salary_dense_rank").over(w_hist).alias("next_salary_dense_rank"),
        F.lag("salary_dense_rank").over(w_hist).alias("prev_salary_dense_rank"),
        F.lead("salary_share").over(w_hist).alias("next_salary_share"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)
