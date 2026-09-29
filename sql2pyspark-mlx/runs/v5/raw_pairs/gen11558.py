from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(", "), "location"))
)

result = (
    valid
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.email").like("%@yahoo.com")), "left_anti")
)
