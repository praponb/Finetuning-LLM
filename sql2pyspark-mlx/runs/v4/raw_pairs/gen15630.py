from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") >= "2023-05-01")
    .withColumn("full_name_title", F.initcap(F.trim("full_name")))
    .withColumn("level_upper", F.upper("level"))
)
