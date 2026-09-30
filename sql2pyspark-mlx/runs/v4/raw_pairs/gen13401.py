from pyspark.sql import functions as F

filtered = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .withColumn("email_full_name", F.concat("email", F.lit(" "), "full_name"))
    .withColumn("level_title", F.initcap(F.trim("level")))
)

result = (
    filtered
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.dept_name"))
)
