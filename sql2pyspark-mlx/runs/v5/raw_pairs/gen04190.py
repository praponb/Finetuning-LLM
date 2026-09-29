from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy("adjusted_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title").orderBy("hire_date_plus_months", "id").rowsBetween(-2, -1)
w3 = Window.orderBy("adjusted_salary", "id")
w_recent = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w5 = Window.partitionBy("job_title", "level").orderBy("salary", "id").rowsBetween(-3, -1)

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-02-13").cast("date")) & F.col("email").isNotNull())
    .select(
        "*",
        F.add_months("hire_date", 3).alias("hire_date_plus_months"),
        F.round((F.col("salary") * 1.1), 2).alias("adjusted_salary"),
        F.coalesce("job_title", F.lit("OTHER")).alias("resolved_job_title"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "full_name",
        "email",
        "salary",
        "hire_date_plus_months",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.avg("salary").over(w2).alias("salary_avg_2_prior"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.avg("salary").over(w5).alias("salary_avg_3_prior"),
    )
)
