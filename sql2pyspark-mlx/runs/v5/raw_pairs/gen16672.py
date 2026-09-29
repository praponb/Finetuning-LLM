from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id")

filtered = (
    df_stores
    .filter(F.col("store_name").like("Ma%"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.trim("store_name").alias("clean_store_name"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

result = (
    filtered
    .filter(F.lower(F.trim("store_name")) == "mall central")
    .select(
        "id",
        "city",
        "sqft",
        F.sum("sqft").over(w).alias("running_sqft"),
        F.ntile(4).over(w2).alias("sqft_bucket"),
    )
)
