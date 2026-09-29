from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id")

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("opened_date") >= F.lit("2023-03-25").cast("date")))
    .select(
        "*",
        F.concat("store_name", F.lit("/"), "region").alias("store_name_region"),
    )
)

result = (
    cleaned
    .filter(F.col("region").isin(["South", "Central", "North"]))
    .withColumn("last3_max_sqft", F.max("sqft").over(w_grp))
    .withColumn("last3_count_sqft", F.count("sqft").over(w_grp))
    .withColumn("last3_min_sqft", F.min("sqft").over(w_grp))
    .withColumn("sqft_bucket", F.ntile(4).over(w2))
)
