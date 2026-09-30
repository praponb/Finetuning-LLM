from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("region").orderBy(F.col("order_discount").desc_nulls_last(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-08-11").cast("date")) & (F.col("sqft") < 13723))
    .withColumn("city_prefix", F.substring("city", 1, 2))
    .withColumn("resolved_region", F.coalesce("region", F.lit("UNKNOWN")))
)

eligible = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id") == 9), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount").alias("order_discount"))
)

cleaned = (
    with_latest
    .filter(F.col("opened_date") >= F.to_date(F.lit("2023-08-13")))
    .withColumn("city_title", F.initcap(F.trim("city")))
    .withColumn("clean_region", F.trim("region"))
    .withColumn("region_clean", F.coalesce("region", F.lit("NONE")))
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "city_prefix",
        "order_discount",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("order_discount") >= 0.39, "HIGH").otherwise("LOW").alias("order_discount_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        "region",
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
