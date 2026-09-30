from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("dept_id", "job_title").orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("job_title", "dept_id").orderBy("salary", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

cleaned = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-03-03")))
    .withColumn("level_full_name_key", F.concat_ws("/", "level", "full_name"))
    .withColumn("job_title_level", F.concat("job_title", F.lit(" "), "level"))
)

scored = (
    cleaned
    .select(
        "*",
        F.round((F.col("salary") / F.sum("salary").over(w)), 4).alias("salary_share"),
        F.last("level", ignorenulls=True).over(w2).alias("last_known_level"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w_part))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked_employees = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked_employees
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "last_known_level",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("dept_id").asc_nulls_first())
)
