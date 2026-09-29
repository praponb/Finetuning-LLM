from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("cnt"),
        F.max("created_at").alias("last_created_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("cnt") >= 1)
    .orderBy("span_minutes")
)
