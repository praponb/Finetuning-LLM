from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("sqft").asc_nulls_last())
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first())
w3 = Window.partitionBy("city")

cleaned = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-04-13").cast("date")) & F.col("region").isNotNull())
    .withColumn("sqft_level", F.when(F.col("sqft") >= 13576, "High").when(F.col("sqft") >= 5247, "Medium").otherwise("Low"))
    .withColumn("region_clean", F.coalesce("region", F.lit("OTHER")))
)

valid = (
    cleaned
    .filter(F.col("opened_date") > F.lit("2023-08-02").cast("date"))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "sqft_level",
        "region_clean",
        F.initcap(F.trim("region")).alias("region_title"),
    )
)

kept_keys = (
    valid
    .select("id", "city")
    .intersect(
        df_stores
        .filter(F.col("region") == "West")
        .select("id", "city")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "region_title",
        F.dense_rank().over(w_recent).alias("sqft_dense_rank"),
        F.percent_rank().over(w2).alias("sqft_percent_rank"),
        F.min("sqft").over(w3).alias("city_min_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)

cleaned_stores = (
    metrics
    .filter(F.col("opened_date") >= F.lit("2023-09-04").cast("date"))
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "region_title",
        "sqft_dense_rank",
        "sqft_percent_rank",
        "city_min_sqft",
        "sqft_share",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

result = (
    cleaned_stores
    .select(
        "city",
        F.col("sqft_percent_rank").alias("SQFT_PERCENT_RANK_VAL"),
        "sqft_share",
    )
    .unpivot(
        ids=["city"],
        values=["SQFT_PERCENT_RANK_VAL", "sqft_share"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
