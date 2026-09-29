from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("opened_date") > F.to_date(F.lit("2023-05-09"))))
    .select(
        "*",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.initcap(F.trim("store_name")).alias("store_name_title"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "city", "region", "opened_date", "sqft", "store_name_title")
)
