from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") != "New York") | (F.col("budget") > 4011000))
    .withColumn("location_prefix", F.substring("location", 1, 3))
    .withColumn("budget_level", F.when(F.col("budget") >= 3386000, "High").when(F.col("budget") >= 1018000, "Medium").otherwise("Low"))
)
