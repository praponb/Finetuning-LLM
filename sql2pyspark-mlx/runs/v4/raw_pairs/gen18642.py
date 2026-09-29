from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("dept_id", "level").orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id")
w5 = Window.orderBy("salary_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

base = (
    df_employees
    .filter(~F.col("level").isin(["L3", "L4", "L5"]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.upper("full_name").alias("full_name_upper"),
        F.concat_ws("-", "email", "level").alias("email_level_key"),
    )
)

scored = (
    base
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("salary_avg_3_prior", F.avg("salary").over(w2))
)

base_employees = (
    scored
    .filter(F.col("dept_id").isin([14, 30]))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "is_remote",
        "level",
        "full_name_upper",
        "email_level_key",
        "salary_share",
        F.lower("full_name").alias("full_name_lower"),
    )
)

scored_employees = (
    base_employees
    .select(
        "*",
        F.min("salary").over(w).alias("level_min_salary"),
        F.last("level", ignorenulls=True).over(w3).alias("last_known_level"),
        F.min("salary").over(w4).alias("dept_id_min_salary"),
        F.last("level", ignorenulls=True).over(w5).alias("last_known_level_2"),
        F.last("level", ignorenulls=True).over(w6).alias("last_known_level_3"),
    )
)

result = (
    scored_employees
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "salary",
        "level",
        "full_name_upper",
        "email_level_key",
        "salary_share",
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("salary").desc(), "dept_id")
)
