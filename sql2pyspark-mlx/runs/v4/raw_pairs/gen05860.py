from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("year", "opened_date").alias("year_start"))
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("sqft_per_tokyo"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.sum("sqft").alias("total_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
)
