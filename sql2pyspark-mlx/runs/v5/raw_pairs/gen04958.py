from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & ~F.col("location").isin(["New York", "Singapore"]))
    .withColumn("dept_name_lower", F.lower("dept_name"))
    .withColumn("location_dept_name", F.concat("location", F.lit("/"), "dept_name"))
)

result = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.level").isin(["L3", "L4"])), "left_anti")
)
