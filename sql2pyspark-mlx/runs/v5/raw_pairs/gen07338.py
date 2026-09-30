from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

cleaned = (
    df_stores
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-07-19")))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.concat_ws("-", "city", "region").alias("city_region_key"),
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

result = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)
