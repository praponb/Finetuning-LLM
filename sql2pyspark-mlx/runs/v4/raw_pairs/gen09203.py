from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([2, 3, 10]))
    .withColumn("region_group", F.when(F.col("region").isin(["East", "North"]), "Priority").otherwise("Other"))
    .withColumn("city_title", F.initcap(F.trim("city")))
    .orderBy("city_title")
)
