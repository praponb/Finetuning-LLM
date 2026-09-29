from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location")
w2 = Window.partitionBy("location").orderBy("hire_date", "id")

valid = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-07-08").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.initcap(F.trim("email")).alias("email_title"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.location"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.dept_name"))
)

with_windows = (
    with_dims
    .filter(F.col("hire_date").between("2023-08-01", "2025-07-19"))
    .select(
        "id",
        "full_name",
        "salary",
        "hire_date",
        "level",
        "is_remote_flag",
        "dept_name",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"), "dept_name")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.max(F.when(F.col("level") == "L4", F.col("salary"))).alias("l4_max_salary"),
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("salary_share")).otherwise(0)).alias("hr_salary_share"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("salary")).otherwise(0)).alias("sales_salary"),
    )
    .orderBy("hr_salary_share")
)
