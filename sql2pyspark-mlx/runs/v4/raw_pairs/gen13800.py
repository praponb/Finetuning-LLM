from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("location_length", F.length("location"))
    .withColumn("location_clean", F.coalesce("location", F.lit("UNKNOWN")))
)

result = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date"))
)
