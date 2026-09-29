from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .orderBy("sqft_range")
)
