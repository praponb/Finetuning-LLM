from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Bangkok", "Chiang Mai"]) & (F.col("country") == "DE"))
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("stddev_rating") < 53)
)
