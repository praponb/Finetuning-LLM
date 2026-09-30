from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .groupingSets([["week_start", "region"], ["week_start"]], "week_start", "region")
    .agg(
        F.count(F.when(F.col("region") == "East", 1)).alias("east_rows"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count("*").alias("num_rows"),
        F.grouping("week_start").alias("grouping_id"),
    )
    .orderBy("east_rows")
)
