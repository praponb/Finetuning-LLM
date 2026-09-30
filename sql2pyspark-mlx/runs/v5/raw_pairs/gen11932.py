from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary_whole")

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-05-31"))) & F.col("job_title").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        F.ceil("salary").alias("salary_whole"),
    )
)

result = (
    cleaned
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "manager_id", "hire_date", "is_remote")
)
