from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

ranked = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_name", "city", "region", "opened_date", "sqft")
)

with_latest = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.sqft",
        F.col("c.discount"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

base = (
    with_latest
    .filter(F.col("region").isNotNull() & (~F.col("region").eqNullSafe(F.lit("Central"))))
    .select("*", F.trim("region").alias("clean_region"))
)

latest_match = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.region", "s.discount", F.col("c.status"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date").between("2023-09-01", "2025-06-01")), "left_semi")
)

result = (
    kept
    .groupBy(
        "region",
        F.when(F.col("discount") >= 0.44, "LARGE").otherwise("SMALL").alias("discount_tier"),
    )
    .agg(
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.countDistinct("region").alias("distinct_regions"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_rows"),
    )
    .orderBy(F.col("region").desc())
)
