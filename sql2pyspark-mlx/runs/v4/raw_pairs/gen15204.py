from pyspark.sql import functions as F

valid = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-08-09").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.trim("region").alias("clean_region"),
        F.initcap(F.trim("region")).alias("region_title"),
    )
)

result = (
    valid
    .groupBy("region")
    .pivot("city", ["New York", "Chiang Mai"])
    .agg(F.sum("sqft"))
    .select(
        "region",
        F.col("New York").alias("new_york"),
        F.col("Chiang Mai").alias("chiang_mai"),
    )
)
