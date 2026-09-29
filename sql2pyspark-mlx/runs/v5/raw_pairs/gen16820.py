from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc(), "id")

filtered = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("hire_date") >= F.lit("2023-08-22").cast("date")))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.round((F.col("salary") * 1.07), 2).alias("salary_scaled"),
        F.coalesce("job_title", F.lit("NONE")).alias("job_title_filled"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "full_name", "job_title", "is_remote_flag", "job_title_filled")
)
