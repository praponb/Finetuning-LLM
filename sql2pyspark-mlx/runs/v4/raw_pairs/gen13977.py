from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc())
w5 = Window.partitionBy("region").orderBy(F.col("sqft_bucket").desc_nulls_last(), "id")
w6 = Window.partitionBy("region").orderBy("sqft_share", "id")
w7 = Window.partitionBy("city").orderBy("sqft_share")

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.upper("city").alias("city_upper"),
        F.trim("city").alias("clean_city"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
    .withColumn("sqft_bucket", F.ntile(5).over(w3))
    .withColumn("sqft_cume_dist", F.cume_dist().over(w4))
)

filtered = (
    enriched
    .filter((F.col("opened_date") > F.lit("2024-03-25").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "city_upper",
        "clean_city",
        "rank_in_group",
        "sqft_share",
        "sqft_bucket",
        "sqft_cume_dist",
        F.when(F.col("city").isin(["New York", "London"]), "Group A").otherwise("Other").alias("city_group"),
        F.coalesce("region", F.lit("NONE")).alias("region_filled"),
    )
)

latest_stores = (
    filtered
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

scored = (
    latest_stores
    .select(
        "*",
        F.lead("sqft_cume_dist", 1).over(w6).alias("next_sqft_cume_dist"),
        F.cume_dist().over(w7).alias("sqft_share_cume_dist"),
    )
)

result = (
    scored
    .select(
        "id",
        "region",
        F.col("next_sqft_cume_dist").alias("NEXT_SQFT_CUME_DIST"),
        F.col("sqft_share").alias("SQFT_SHARE"),
        F.col("sqft_share_cume_dist").alias("SQFT_SHARE_CUME_DIST"),
    )
    .unpivot(
        ["id", "region"],
        ["NEXT_SQFT_CUME_DIST", "SQFT_SHARE", "SQFT_SHARE_CUME_DIST"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
