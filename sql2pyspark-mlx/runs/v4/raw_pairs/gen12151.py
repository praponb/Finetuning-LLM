from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .withColumn("level_lower", F.lower("level"))
    .withColumn("email_full_name", F.concat("email", F.lit(", "), "full_name"))
    .orderBy("dept_id")
)
