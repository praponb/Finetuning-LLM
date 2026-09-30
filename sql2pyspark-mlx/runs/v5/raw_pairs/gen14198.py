from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "London") & F.col("location").isNull())
    .withColumn("is_singapore", F.when(F.col("location") == "Singapore", 1).otherwise(0))
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" "), "location"))
)
