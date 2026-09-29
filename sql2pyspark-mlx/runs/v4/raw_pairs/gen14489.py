from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") == 15741)
    .withColumn("region_group", F.when(F.col("region").isin(["East", "South"]), "Core").otherwise("Other"))
)
