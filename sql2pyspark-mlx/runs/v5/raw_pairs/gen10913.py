from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc_nulls_first(), "id")

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-03-12"))) & F.col("region").isNotNull())
    .select(
        "*",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

result = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_name", "sqft", "sqft_filled", "resolved_region")
)
