from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("%Ford%"))
    .select("id", "salary")
    .orderBy(F.col("id").desc())
    .limit(50)
)
