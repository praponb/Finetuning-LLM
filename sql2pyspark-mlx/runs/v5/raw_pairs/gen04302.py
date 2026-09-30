from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("level").orderBy("hire_date", "id")

cleaned = (
    df_employees
    .filter((~F.col("level").eqNullSafe(F.lit("L4"))) & (F.col("hire_date") > F.lit("2023-07-22").cast("date")))
    .select(
        "id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.initcap(F.trim("job_title")).alias("job_title_title"),
        F.upper(F.trim("full_name")).alias("full_name_normalized"),
    )
)

result = (
    cleaned
    .withColumn("prev_salary", F.lag("salary").over(w_recent))
    .withColumn("next_salary", F.lead("salary").over(w_recent))
    .withColumn("row_seq", F.row_number().over(w_recent))
)
