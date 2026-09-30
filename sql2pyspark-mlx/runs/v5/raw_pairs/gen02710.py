from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy("salary")
w2 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(-5, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-07-31").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.length("job_title").alias("job_title_length"),
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "full_name",
        "manager_id",
        "hire_date",
        "is_remote",
        "job_title_length",
        "email_user",
        F.dense_rank().over(w).alias("salary_dense_rank"),
        F.avg("salary").over(w2).alias("salary_avg_5"),
    )
)
