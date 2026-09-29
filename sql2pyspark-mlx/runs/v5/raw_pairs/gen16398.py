from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location", "dept_name").orderBy("hire_date", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("location").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy(F.col("salary_avg_3_prior").desc(), "id")
w4 = Window.partitionBy("location").orderBy("salary")
w5 = Window.partitionBy("level").orderBy(F.col("cumulative_salary").desc(), "id")
w6 = Window.partitionBy("dept_name", "dept_id").orderBy(F.col("cumulative_salary").desc(), "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

with_dims = (
    df_employees
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .filter(F.col("b.budget") < 3291000)
    .select("a.*", F.col("b.location"), F.col("b.dept_name"))
)

enriched = (
    with_dims
    .withColumn("salary_avg_3_prior", F.avg("salary").over(w))
    .withColumn("cumulative_salary", F.sum("salary").over(w2))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    deduped
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
)

ranked_employees = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "level",
        "dept_name",
        "cumulative_salary",
    )
)

ranked_employees_2 = (
    ranked_employees
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked_employees_2
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "level",
        "dept_name",
        "row_num",
        F.when(F.col("dept_name").rlike("^(Legal|Engineering)$"), "PRIORITY").when(F.col("dept_name").rlike("^(Sales|Marketing)$"), "GROUP_B").otherwise("EXTENDED").alias("dept_name_zone"),
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("dept_name").desc_nulls_first(), F.col("job_title").desc())
)
