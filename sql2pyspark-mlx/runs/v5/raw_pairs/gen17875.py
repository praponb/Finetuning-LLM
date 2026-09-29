from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc())
w3 = Window.partitionBy("city", "region").orderBy(F.col("sqft_rank").desc())

remaining_keys = (
    df_stores
    .select("city", "id")
    .subtract(
        df_stores
        .filter(F.col("city").isin(["Bangkok", "Berlin"]))
        .select("city", "id")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.avg("sqft").over(w).alias("city_avg_sqft"),
        F.rank().over(w2).alias("sqft_rank"),
        F.cume_dist().over(w2).alias("sqft_cume_dist"),
    )
)

remaining_keys_2 = (
    metrics
    .select("region", "id")
    .intersect(
        df_stores
        .filter(F.col("region").isin(["East", "Central", "West"]))
        .select("region", "id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "store_name",
        "city",
        "opened_date",
        "city_avg_sqft",
        "sqft_cume_dist",
        F.dense_rank().over(w3).alias("sqft_rank_dense_rank"),
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
    )
)

summary = (
    enriched
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.round(F.avg("sqft_cume_dist"), 2).alias("avg_sqft_cume_dist"),
        F.sum("sqft_cume_dist").alias("total_sqft_cume_dist"),
    )
    .filter(F.col("avg_sqft_cume_dist") > 0)
)

result = (
    summary
    .select(
        "city",
        "bangkok_rows",
        "avg_sqft_cume_dist",
        "total_sqft_cume_dist",
        F.when(F.col("city").rlike("^(New York|Berlin)$"), "CORE").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc_nulls_last())
)
