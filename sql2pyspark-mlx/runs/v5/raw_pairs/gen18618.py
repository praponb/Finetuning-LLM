from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id").rowsBetween(-3, -1)
w_ordered = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("job_title").orderBy("salary", "id")
w4 = Window.partitionBy("level", "job_title").orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("job_title", "manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("full_name").like("%Sris%"))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        F.avg("salary").over(w).alias("salary_avg_3_prior"),
        F.dense_rank().over(w_ordered).alias("salary_dense_rank"),
        F.lead("salary", 2).over(w3).alias("next_salary"),
        F.ntile(10).over(w4).alias("salary_bucket"),
        F.first("level", ignorenulls=True).over(w5).alias("first_known_level"),
    )
)
