from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy("sqft", "id")
w2 = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc())
w3 = Window.orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("region", "city").orderBy("rank_in_group", "id")
w5 = Window.partitionBy("city").orderBy(F.col("sqft_bucket").asc_nulls_last(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("city").orderBy(F.col("latest_rank").desc(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-07-03"))) & F.col("region").isNotNull() & (F.col("sqft") >= 5432))
    .select("*", F.concat("city", F.lit(" "), "region").alias("city_region"))
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .withColumn("sqft_percent_rank", F.percent_rank().over(w2))
    .withColumn("sqft_bucket", F.ntile(10).over(w3))
    .withColumn("next_sqft", F.lead("sqft", 1, 0).over(w4))
)

filtered_stores = (
    scored
    .filter(F.col("opened_date") >= F.lit("2023-10-16").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "city_region",
        "rank_in_group",
        "sqft_bucket",
        "next_sqft",
        (F.col("sqft_percent_rank") + F.col("next_sqft")).alias("sqft_percent_rank_plus_next_sqft"),
        (F.col("next_sqft") - F.col("rank_in_group")).alias("next_sqft_minus_rank_in_group"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_filled"),
    )
)

latest = (
    filtered_stores
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

latest_match = (
    latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"))
)

deduped_stores = (
    latest_match
    .withColumn("latest_rank_2", F.row_number().over(w7))
    .filter(F.col("latest_rank_2") == 1)
)

result = (
    deduped_stores
    .select(
        "id",
        "city",
        "sqft",
        "city_region",
        "rank_in_group",
        "sqft_bucket",
        "next_sqft",
        "sqft_percent_rank_plus_next_sqft",
        "next_sqft_minus_rank_in_group",
        F.when(F.col("city").rlike("^(Chiang Mai|Tokyo)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|New York)$"), "PRIORITY").otherwise("CORE").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc_nulls_last(), F.col("id").desc())
    .limit(10)
)
