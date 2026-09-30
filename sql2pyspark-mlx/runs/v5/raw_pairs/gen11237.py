from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "sale"))
    .withColumn("sorted_tags", F.sort_array("tags"))
    .withColumn("is_initech", F.when(F.col("brand") == "Initech", 1).otherwise(0))
)
