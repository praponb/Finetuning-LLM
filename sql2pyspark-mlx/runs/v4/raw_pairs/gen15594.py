from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("employee_level").orderBy("salary_scaled", "id")
w2 = Window.partitionBy("dept_name").orderBy("budget", "id").rowsBetween(-5, Window.currentRow)

base = (
    df_employees
    .filter(F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.length("email").alias("email_length"),
        F.round((F.col("salary") * 0.85), 2).alias("salary_scaled"),
        F.coalesce("level", F.lit("DIRECT")).alias("level_filled"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.dept_name"), F.col("b.budget"))
)

joined_employees = (
    joined
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.level").alias("employee_level"))
)

scored = (
    joined_employees
    .select(
        "*",
        F.ntile(3).over(w).alias("salary_scaled_bucket"),
        F.avg("email_length").over(w2).alias("email_length_avg_5"),
    )
)

cleaned = (
    scored
    .filter(F.col("level").isNotNull() & (F.col("salary_scaled_bucket") >= 8))
    .select(
        "*",
        (F.col("salary_scaled_bucket") / F.nullif("email_length", F.lit(0))).alias("salary_scaled_bucket_per_email_length"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_clean"),
    )
)

result = (
    cleaned
    .groupBy("employee_level", "manager_id")
    .pivot("dept_name", ["HR", "Finance"])
    .agg(
        F.sum("salary_scaled_bucket").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
