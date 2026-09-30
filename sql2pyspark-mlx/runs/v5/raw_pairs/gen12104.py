from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.when(F.col("sqft") >= 17507, "HIGH").otherwise("LOW").alias("sqft_tier"))
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles_2"),
    )
    .filter(F.col("cnt") > 1)
    .orderBy(F.col("sqft_tier").desc_nulls_first())
)
