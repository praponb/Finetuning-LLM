from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .cube(
        "region",
        F.when(F.col("sqft") >= 15421, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("sqft_per_berlin"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
        F.countDistinct("region").alias("distinct_regions"),
    )
)
