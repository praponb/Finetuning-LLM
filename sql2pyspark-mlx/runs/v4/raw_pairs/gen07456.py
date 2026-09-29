from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("city").isin(["Chiang Mai", "Bangkok"]) & ((F.length("supplier_name") >= 6) | (F.col("city") == "Berlin")))
    .withColumn("supplier_name_slug", F.regexp_replace("supplier_name", " ", ""))
)

city_summary = (
    src
    .groupBy(
        "city",
        F.when(F.col("country").isin(["UK", "JP"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
)

result = (
    city_summary
    .filter(F.col("stddev_rating") >= 59)
    .select("city", "country_group", "stddev_rating")
    .orderBy(F.col("country_group").desc())
)
