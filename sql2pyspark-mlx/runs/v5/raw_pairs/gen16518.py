from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("salary", "id")
w3 = Window.partitionBy("level", "dept_id")
w4 = Window.partitionBy("dept_id", "dept_name").orderBy("next_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("dept_name").orderBy("next_salary", "id")
w7 = Window.partitionBy("dept_id", "dept_name")
w8 = Window.orderBy(F.col("salary").desc(), "id")

valid = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-06-12").cast("date")) & (F.col("job_title").eqNullSafe(F.lit("Intern"))))
    .withColumn("job_title_title", F.initcap(F.trim("job_title")))
)

scored = (
    valid
    .select(
        "*",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.lead("salary", 1).over(w2).alias("next_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
    )
)

with_dims = (
    scored
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.email",
        "e.dept_id",
        "e.salary",
        "e.is_remote",
        "e.job_title_title",
        "e.next_salary",
        "e.salary_share",
        F.col("d.dept_name"),
    )
)

scored_employees = (
    with_dims
    .filter(F.col("email").isNull())
    .select(
        "*",
        F.sum("salary_share").over(w4).alias("cumulative_salary_share"),
        F.last("salary_share", ignorenulls=True).over(w5).alias("last_known_salary_share"),
    )
)

with_windows = (
    scored_employees
    .select(
        "*",
        F.lead("next_salary", 2).over(w6).alias("next_next_salary"),
        F.round((F.col("next_salary") / F.sum("next_salary").over(w7)), 4).alias("next_salary_share"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

result = (
    with_windows
    .groupBy("dept_name")
    .agg(
        F.count(F.when(F.col("dept_name") == "Engineering", 1)).alias("engineering_count"),
        F.round(F.avg("salary_share"), 2).alias("avg_salary_share"),
        F.count(F.when(F.col("dept_name") == "Marketing", 1)).alias("marketing_rows"),
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("last_known_salary_share")).otherwise(0)).alias("legal_last_known_salary_share"),
        F.count_if(F.col("dept_name") == "Sales").alias("sales_events"),
        F.round(F.sum("next_salary") / F.nullif(F.count(F.when(F.col("dept_name") == "Sales", 1)), F.lit(0)), 2).alias("next_salary_per_sales"),
    )
)
