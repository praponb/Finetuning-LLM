from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc_nulls_first())

latest = (
    df_stores
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest_stores = (
    latest
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

valid = (
    latest_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-03-29"))) & F.col("region").isNotNull() & (F.col("sqft") == 6036))
    .select(
        "*",
        F.trim("city").alias("clean_city"),
        F.length("city").alias("city_length"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "clean_city",
        F.when(F.col("city").rlike("^(Singapore|Chiang Mai)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("sqft").asc_nulls_first(), F.col("id").desc())
    .limit(5)
)
