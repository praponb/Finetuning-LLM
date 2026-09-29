from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "attributes",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("ship_to.country").alias("ship_country"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count_if(F.col("ship_country") == "US").alias("us_events"),
        F.sort_array(F.collect_set("ship_country")).alias("ship_country_list"),
        F.sum("items_total").alias("total_items_total"),
        F.min("created_at").alias("first_created_at"),
        F.max("created_at").alias("last_created_at"),
    )
    .orderBy(F.col("total_items_total").desc())
)
