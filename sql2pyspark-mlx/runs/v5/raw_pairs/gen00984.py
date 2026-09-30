from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 12548)
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"), "region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .filter(F.col("sqft_range") >= 29)
)
