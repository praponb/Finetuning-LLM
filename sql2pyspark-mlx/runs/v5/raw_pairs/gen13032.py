from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id")

filtered = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-04-16").cast("date"))
    .select(
        "*",
        F.coalesce("store_name", F.lit("N/A")).alias("store_name_filled"),
        F.initcap(F.trim("region")).alias("region_title"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled"),
    )
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
