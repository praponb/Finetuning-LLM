from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Tokyo", "Berlin", "Singapore"]) | F.col("city").isin(["Bangkok", "Tokyo", "New York"]))
    .withColumn("is_tokyo", F.when(F.col("city") == "Tokyo", 1).otherwise(0))
    .withColumn("city_title", F.initcap(F.trim("city")))
)
