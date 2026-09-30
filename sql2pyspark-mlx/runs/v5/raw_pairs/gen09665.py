from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "country",
        "city",
        "name",
        F.concat_ws(",", "city", "segment").alias("city_segment_key"),
    )
    .orderBy("city_segment_key")
)
