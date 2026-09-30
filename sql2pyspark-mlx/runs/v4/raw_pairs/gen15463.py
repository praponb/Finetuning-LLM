from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.size("items").alias("num_items"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        F.min("created_at").alias("started_at"),
        F.min("created_at").alias("started_at_2"),
    )
    .orderBy(F.col("hour_window").asc_nulls_first())
)
