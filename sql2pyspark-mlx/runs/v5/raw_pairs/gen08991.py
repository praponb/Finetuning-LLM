from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")

cleaned = (
    df_stores
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-09-16")))
    .withColumn("store_name_region", F.concat("store_name", F.lit(" - "), "region"))
    .withColumn("sqft_level", F.when(F.col("sqft") >= 16649, "High").when(F.col("sqft") >= 6226, "Medium").otherwise("Low"))
)

filtered = (
    cleaned
    .filter((F.col("opened_date") > F.lit("2023-08-04").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "sqft_level",
        F.when(F.col("sqft") >= 15436, "High").when(F.col("sqft") >= 5957, "Medium").otherwise("Low").alias("sqft_tier"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.customer_id").isNull()), "left_semi")
)

base = (
    kept
    .filter((~F.col("region").eqNullSafe(F.lit("East"))) & F.col("store_name").like("Ai%"))
    .select("*", F.lower("city").alias("city_lower"))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

stats = (
    deduped
    .groupBy("region")
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("sqft_per_london"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("sqft_per_chiang_mai"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("sqft_per_london") >= 0)
)

result = (
    stats
    .select("region", "sqft_per_london", "sqft_per_chiang_mai")
    .orderBy(F.col("sqft_per_chiang_mai").asc_nulls_first())
)
