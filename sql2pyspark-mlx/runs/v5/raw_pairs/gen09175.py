from pyspark.sql import functions as F

remaining_keys = (
    df_suppliers
    .select("city", "id")
    .intersectAll(
        df_suppliers
        .filter(~F.col("city").isin(["London", "Berlin", "Chiang Mai"]))
        .select("city", "id")
    )
)

reconciled = (
    df_suppliers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("rating_tier", F.when(F.col("rating") >= 4.1, "HIGH").otherwise("LOW"))
    .rollup("city", "rating_tier")
    .agg(
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.sum(F.when(F.col("country") == "TH", F.col("rating"))).alias("th_total_rating"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_rows"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.grouping("city").alias("gid"),
    )
    .orderBy(F.col("th_total_rating").desc_nulls_last())
)
