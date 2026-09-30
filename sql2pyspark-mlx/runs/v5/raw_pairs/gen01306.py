from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "category",
        F.when(F.col("price") >= 1310, "LARGE").otherwise("SMALL").alias("price_tier"),
    )
    .agg(
        F.countDistinct("brand").alias("distinct_brands"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
    )
    .filter(F.col("num_brands") < 1000)
)
