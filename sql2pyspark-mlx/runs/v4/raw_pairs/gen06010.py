from pyspark.sql import functions as F

result = (
    df_departments
    .select("id", "location", "budget")
    .orderBy(F.col("id").desc())
)
