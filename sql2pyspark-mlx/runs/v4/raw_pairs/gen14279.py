from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"), "city")
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "East", 1)), F.lit(0)), 2).alias("sqft_per_east"),
        F.avg(F.when(F.col("region") == "South", F.col("sqft"))).alias("south_avg_sqft"),
    )
)
