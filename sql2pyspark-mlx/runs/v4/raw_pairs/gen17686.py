from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .select("level")
    .unionByName(df_employees.filter(F.col("salary") > 105000).select("level"))
    .distinct()
)
