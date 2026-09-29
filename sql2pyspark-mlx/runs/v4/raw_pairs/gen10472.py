from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-11-25").cast("date")) & F.col("salary").between(131000, 145000))
    .select(
        "*",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("job_title", F.lit("OTHER")).alias("resolved_job_title"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
