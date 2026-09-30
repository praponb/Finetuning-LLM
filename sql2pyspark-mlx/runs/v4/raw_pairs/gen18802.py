from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("level").isin(["L4", "L5"]) | F.col("job_title").isin(["Director", "Engineer"]))
    .withColumn("full_name_level", F.concat("full_name", F.lit(" - "), "level"))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
)
