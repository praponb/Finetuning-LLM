from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNotNull())
    .orderBy("budget", "location")
)
