from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "job_title").orderBy(F.col("hire_date").desc(), "id")

valid = (
    df_employees
    .filter(F.col("salary") >= 111000)
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        F.initcap(F.trim("job_title")).alias("job_title_title"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)
