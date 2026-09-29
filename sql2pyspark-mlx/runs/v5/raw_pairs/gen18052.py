from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w4 = Window.partitionBy("city")
w_ordered = Window.orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

enriched = (
    df_stores
    .select(
        "id",
        "sqft",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.first("region", ignorenulls=True).over(w2).alias("first_known_region"),
        F.sum("sqft").over(w).alias("region_sum_sqft"),
        F.row_number().over(w3).alias("event_seq"),
        F.round((F.col("sqft") / F.sum("sqft").over(w4)), 4).alias("sqft_share_2"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("sqft_share").over(w_ordered).alias("last7_avg_sqft_share"),
        F.min("sqft_share").over(w_ordered).alias("last7_min_sqft_share"),
        F.max("sqft_share").over(w_ordered).alias("last7_max_sqft_share"),
    )
)

base = (
    metrics
    .filter(F.col("sqft_share_2").isNotNull())
    .select(
        "id",
        "sqft",
        "sqft_share",
        "first_known_region",
        "region_sum_sqft",
        "event_seq",
        "last7_avg_sqft_share",
        F.least("sqft_share_2", "region_sum_sqft").alias("least_sqft_share_2_region_sum_sqft"),
        F.greatest("last7_min_sqft_share", "region_sum_sqft").alias("greatest_last7_min_sqft_share_region_sum_sqft"),
    )
)

cleaned = (
    base
    .filter(F.col("least_sqft_share_2_region_sum_sqft") < 36)
    .select(
        "id",
        "sqft",
        "region_sum_sqft",
        "event_seq",
        "last7_avg_sqft_share",
        "least_sqft_share_2_region_sum_sqft",
        "greatest_last7_min_sqft_share_region_sum_sqft",
        F.round("last7_avg_sqft_share", 1).alias("last7_avg_sqft_share_rounded"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isin([4, 5, 6])), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.store_sale_sale_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "sqft",
        "region_sum_sqft",
        "event_seq",
        "last7_avg_sqft_share",
        "least_sqft_share_2_region_sum_sqft",
        "store_sale_sale_date",
    )
    .orderBy(F.col("sqft").asc_nulls_first(), "region_sum_sqft")
)
