from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "New York") | F.col("location").isin(["Tokyo", "Chiang Mai", "London"]))
    .select("id", "budget")
    .orderBy(F.col("id").desc())
)
