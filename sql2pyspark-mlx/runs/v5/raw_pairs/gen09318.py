from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w_part = Window.partitionBy("region").orderBy("sqft", "id")
w_recent = Window.partitionBy("city")

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-07-22").cast("date")) & F.col("opened_date").between("2024-02-18", "2024-05-13"))
    .select(
        "*",
        F.when(F.col("region").isin(["Central", "North"]), "Tier 1").otherwise("Other").alias("region_group"),
        F.concat("region", F.lit(" "), "store_name").alias("region_store_name"),
    )
)

result = (
    filtered
    .select(
        "id",
        "city",
        "opened_date",
        "region_group",
        "region_store_name",
        F.ntile(4).over(w).alias("sqft_bucket"),
        F.lead("sqft", 1).over(w_part).alias("next_sqft"),
        F.count("sqft").over(w_recent).alias("city_count_sqft"),
    )
)
