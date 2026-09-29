from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .withColumn("budget_adjusted", F.round((F.col("budget") * 100), 2))
)
