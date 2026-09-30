from pyspark.sql import functions as F

result = (
    df_suppliers
    .cube(
        F.when(F.col("rating") >= 3.5, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
        "city",
    )
    .agg(
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("london_events") > 1)
)
