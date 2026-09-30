from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("job_title") == "Director") | (F.length("full_name") > 12))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
    .withColumn("hire_date_weekofyear", F.weekofyear("hire_date"))
)
