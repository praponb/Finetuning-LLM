from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first(), "id")
w2 = Window.orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("city_length", "id")

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "*",
        F.length("city").alias("city_length"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    deduped
    .filter(F.col("region").isNotNull())
    .withColumn("store_name_lower", F.lower("store_name"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.avg("sqft").over(w2).alias("sqft_avg_2"),
        F.lag("sqft", 2).over(w3).alias("prev_sqft"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "city_length",
        "resolved_region",
        "row_num",
    )
    .orderBy(F.col("region").asc_nulls_last())
)
