from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("city").orderBy(F.col("cumulative_sqft").asc_nulls_last(), "id")
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("region", "city").orderBy("sqft_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city", "region").orderBy("sqft_share_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city", "region").orderBy("running_cumulative_sqft", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.substring("city", 1, 2).alias("city_prefix"),
        F.substring("city", 1, 2).alias("city_prefix_2"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.opened_date")), "left_anti")
)

with_windows = (
    not_flagged
    .withColumn("cumulative_sqft", F.sum("sqft").over(w))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .filter(F.col("sqft") < 6037)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "city_prefix_2",
        "sqft_share",
        "rn",
        F.round((F.col("sqft") / F.sum("sqft").over(w4)), 4).alias("sqft_share_2"),
        F.sum("cumulative_sqft").over(w5).alias("running_cumulative_sqft"),
    )
)

scored_stores = (
    scored
    .select(
        "id",
        "store_name",
        "city",
        "sqft_share",
        "rn",
        "sqft_share_2",
        "running_cumulative_sqft",
        F.sum("running_cumulative_sqft").over(w6).alias("cumulative_running_cumulative_sqft"),
        F.avg("running_cumulative_sqft").over(w7).alias("running_cumulative_sqft_avg_2"),
    )
)

with_last = (
    scored_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w8))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.shipping_fee"))
)

result = (
    with_last
    .select(
        "id",
        "store_name",
        "city",
        "sqft_share",
        "rn",
        "sqft_share_2",
        "cumulative_running_cumulative_sqft",
        "running_cumulative_sqft_avg_2",
        "shipping_fee",
        F.when(F.col("city").rlike("^(London|New York)$"), "PRIORITY").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("running_cumulative_sqft_avg_2").desc())
)
