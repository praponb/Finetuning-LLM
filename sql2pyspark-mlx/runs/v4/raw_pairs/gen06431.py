from pyspark.sql import functions as F

base = (
    df_employees
    .filter(~(F.col("email").isNull() | F.lower("full_name").like("%avi%")))
    .withColumn("email_lower", F.lower("email"))
)

agg = (
    base
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.countDistinct("salary").alias("unique_depts"),
    )
)

result = (
    agg
    .filter(F.col("unique_depts") < 5)
    .select("hire_year", "min_hire_date", "unique_depts")
)
