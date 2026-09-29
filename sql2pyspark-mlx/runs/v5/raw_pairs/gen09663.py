from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")
w2 = Window.partitionBy("manager_id", "dept_id")
w3 = Window.partitionBy("level")
w4 = Window.partitionBy("manager_id", "dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("job_title")

result = (
    df_employees
    .select(
        "id",
        "full_name",
        "email",
        "job_title",
        "hire_date",
        F.cume_dist().over(w).alias("salary_cume_dist"),
        F.round((F.col("salary") / F.sum("salary").over(w2)), 4).alias("salary_share"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share_2"),
        F.first("job_title", ignorenulls=True).over(w4).alias("first_known_job_title"),
        F.round((F.col("salary") / F.sum("salary").over(w5)), 4).alias("salary_share_3"),
    )
)
