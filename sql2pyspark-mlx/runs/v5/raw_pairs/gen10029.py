from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("month", "opened_date").alias("activity_month"))
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles_2"),
    )
    .filter(F.col("num_regions") > 1)
    .orderBy("num_regions")
)
