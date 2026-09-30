from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy("hire_date", "id")

filtered = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "*",
        F.date_trunc("WEEK", "hire_date").alias("hire_date_period"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("resolved_level"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
