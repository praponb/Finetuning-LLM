from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title")
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy(F.col("salary_share_2").desc(), "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

scored = (
    df_employees
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("salary_share_2", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("running_salary", F.sum("salary").over(w2))
)

cleaned = (
    scored
    .filter(F.col("manager_id") == 21)
    .withColumn("job_title_level", F.concat("job_title", F.lit(", "), "level"))
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    ranked
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "salary_share",
        F.col("min_employee_salary"),
    )
)
