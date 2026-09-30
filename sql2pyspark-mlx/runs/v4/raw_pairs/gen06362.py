from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 3396000)
    .select("id", "location", "budget")
    .orderBy("budget")
)
