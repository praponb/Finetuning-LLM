from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary_scaled").desc_nulls_first(), "id")

filtered = (
    df_employees
    .filter(F.col("job_title").isNotNull() & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        F.round((F.col("salary") * 1.1), 2).alias("salary_scaled"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "email", "manager_id", "job_title", "salary_scaled")
)
