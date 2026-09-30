from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("job_title").orderBy("salary", "id")

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-08-04").cast("date")) & F.col("job_title").isNotNull())
    .withColumn("job_title_filled", F.coalesce("job_title", F.lit("N/A")))
    .withColumn("level_filled", F.coalesce("level", F.lit("OTHER")))
)

result = (
    cleaned
    .select(
        "id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        "level_filled",
        F.lag("salary").over(w_hist).alias("prev_salary"),
        F.lead("salary").over(w_hist).alias("next_salary"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)
