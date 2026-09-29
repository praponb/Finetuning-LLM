from pyspark.sql import functions as F

remaining_keys = (
    df_stores
    .select("id")
    .intersect(
        df_stores
        .filter(F.col("city").isin(["Berlin", "New York", "London"]))
        .select("id")
    )
)

verified = (
    df_stores
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(F.date_trunc("MONTH", "opened_date").alias("activity_month"), "city")
    .agg(
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_rows"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.countDistinct("region").alias("distinct_regions"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .filter(F.col("london_rows") < 1)
    .orderBy(F.col("num_cities").desc_nulls_last())
)
