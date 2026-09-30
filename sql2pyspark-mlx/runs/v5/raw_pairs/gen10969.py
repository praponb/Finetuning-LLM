from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.year("opened_date") == 2023) & F.col("store_name").isNotNull())
    .withColumn("region_store_name", F.concat("region", F.lit(", "), "store_name"))
)

agg = (
    src
    .groupBy("region")
    .agg(F.sum("sqft").alias("total_sqft"))
)

result = (
    agg
    .filter(F.col("total_sqft") >= 40000)
    .orderBy(F.col("total_sqft").desc(), F.col("region").desc())
)
