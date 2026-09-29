from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date")
w2 = Window.partitionBy("city")
w3 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-08-19").cast("date")) & F.col("region").isNotNull())
    .withColumn("region_lower", F.lower("region"))
)

ranked = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    ranked
    .filter(F.col("opened_date") > F.lit("2023-09-20").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "rn",
        F.round((F.col("rn") / F.col("sqft")), 2).alias("rn_per_sqft"),
        F.floor("rn").alias("rn_whole"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.sum("sqft").over(w2).alias("city_sum_sqft"),
        F.sum("sqft").over(w3).alias("cumulative_sqft"),
        F.last("sqft", ignorenulls=True).over(w4).alias("last_known_sqft"),
    )
)

result = (
    metrics
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "rn",
        "rn_per_sqft",
        "rn_whole",
        "city_sum_sqft",
        "cumulative_sqft",
    )
    .orderBy(F.col("region").desc())
)
