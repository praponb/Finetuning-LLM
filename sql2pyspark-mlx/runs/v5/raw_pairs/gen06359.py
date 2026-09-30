from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w_hist = Window.orderBy("sqft", "id")
w4 = Window.partitionBy("region", "city").orderBy("sqft_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city")

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-06-26"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
        F.concat_ws("-", "region", "city").alias("region_city_key"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.customer_id") == 22), "left_anti")
)

with_windows = (
    not_flagged
    .filter(F.col("store_name").like("Old%"))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("next_sqft", F.lead("sqft", 1).over(w2))
    .withColumn("next_sqft_2", F.lead("sqft", 1).over(w_hist))
)

kept = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.product_id").isin([2, 5, 7])), "left_anti")
)

enriched = (
    kept
    .withColumn("first_known_next_sqft", F.first("next_sqft", ignorenulls=True).over(w4))
    .withColumn("sqft_share_2", F.round((F.col("sqft") / F.sum("sqft").over(w5)), 4))
)

grouped = (
    enriched
    .cube("region")
    .agg(
        F.round(F.sum("first_known_next_sqft") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("first_known_next_sqft_per_bangkok"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.percentile_approx("next_sqft", [0.25, 0.5, 0.75]).alias("next_sqft_quartiles"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft_share"))).alias("chiang_mai_total_sqft_share"),
        F.round(F.sum("sqft_share") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("sqft_share_per_bangkok"),
    )
)

result = (
    grouped
    .select(
        "region",
        "first_known_next_sqft_per_bangkok",
        "sqft_bit_and",
        "next_sqft_quartiles",
        F.when(F.col("region").rlike("^(South|North)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("region_zone"),
    )
    .orderBy(F.col("sqft_bit_and").desc(), "region")
)
