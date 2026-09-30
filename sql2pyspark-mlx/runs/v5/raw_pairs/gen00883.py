from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("level").orderBy(F.col("hire_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy("hire_date", "id")

filtered = (
    df_employees
    .filter((~F.col("level").eqNullSafe(F.lit("L3"))) & F.col("salary").between(137000, 143000))
    .withColumn("level_prefix", F.substring("level", 1, 3))
)

cleaned = (
    filtered
    .filter(F.col("hire_date") >= F.lit("2023-05-19").cast("date"))
    .withColumn("job_title_lower", F.lower("job_title"))
    .withColumn("hire_date_period", F.date_trunc("YEAR", "hire_date"))
)

scored = (
    cleaned
    .withColumn("prev_salary", F.lag("salary", 2).over(w))
    .withColumn("salary_max_7d", F.max("salary").over(w2))
)

with_windows = (
    scored
    .withColumn("next_prev_salary", F.lead("prev_salary").over(w3))
    .withColumn("prev_prev_salary", F.lag("prev_salary").over(w3))
    .withColumn("next_salary_max_7d", F.lead("salary_max_7d").over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
)

result = (
    with_windows
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "level_prefix",
        "hire_date_period",
        "prev_salary",
    )
    .orderBy("job_title")
)
