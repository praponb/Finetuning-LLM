from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("created_at") >= "2024-06-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        F.col("ship_to.city").alias("ship_city"),
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
    )
)

result = (
    flattened_attrs
    .withColumn("items_total_tier", F.when(F.col("items_total") >= 1430, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["items_total_tier", "ship_city"], ["items_total_tier"]],
        "items_total_tier",
        "ship_city",
    )
    .agg(
        F.count(F.when(F.col("ship_city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.min("created_at").alias("first_created_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("ship_city") == "London", 1)).alias("london_rows"),
        F.grouping("items_total_tier").alias("grouping_id"),
    )
    .orderBy("bangkok_rows")
)
