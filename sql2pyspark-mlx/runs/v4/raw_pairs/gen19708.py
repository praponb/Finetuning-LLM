from pyspark.sql import functions as F

matched_keys = (
    df_suppliers
    .select("city", "country")
    .exceptAll(
        df_suppliers
        .filter(~F.col("supplier_name").like("%EuroG%"))
        .select("city", "country")
    )
)

verified = (
    df_suppliers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("country")
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating"))).alias("chiang_mai_total_rating"),
        F.stddev_samp("rating").alias("rating_stddev_2"),
        F.stddev_samp("rating").alias("rating_stddev_3"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy(F.col("london_rows").desc_nulls_last())
)
