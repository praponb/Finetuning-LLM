from pyspark.sql import functions as F

eligible = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.opened_date")), "left_semi")
)

result = (
    eligible
    .groupBy(F.date_trunc("year", "opened_date").alias("year_start"))
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
    )
)
