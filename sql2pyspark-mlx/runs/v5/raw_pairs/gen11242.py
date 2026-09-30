from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "Tokyo", "London"]) & F.col("dept_name").isin(["Finance", "Marketing"]))
    .withColumn("budget_level", F.when(F.col("budget") >= 3060000, "High").when(F.col("budget") >= 1973000, "Medium").otherwise("Low"))
    .withColumn("budget_int", F.col("budget").cast("int"))
)
