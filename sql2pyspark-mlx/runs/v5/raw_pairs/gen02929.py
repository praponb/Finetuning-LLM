from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 2322000)
    .select("location")
    .unionByName(df_departments.filter(F.col("location").isNull()).select("location"))
    .distinct()
)
