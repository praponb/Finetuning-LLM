from pyspark.sql import functions as F

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "clearance")), "left_anti")
)

result = (
    kept
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles_2"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
