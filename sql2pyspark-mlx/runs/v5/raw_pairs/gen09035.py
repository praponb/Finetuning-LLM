from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft")
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("opened_date", "id")
w4 = Window.partitionBy("city").orderBy("opened_date", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w6 = Window.orderBy("sqft_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city", "region").orderBy("inventory_quantity", "id")
w_ordered = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

enriched = (
    df_stores
    .withColumn("sqft_percent_rank", F.percent_rank().over(w))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w2))
    .withColumn("next_sqft", F.lead("sqft", 1).over(w3))
    .withColumn("next_sqft_2", F.lead("sqft", 2).over(w4))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("inventory_quantity"))
)

enriched_stores = (
    with_latest
    .filter(F.col("store_name").like("D%"))
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "next_sqft",
        "next_sqft_2",
        F.last("next_sqft", ignorenulls=True).over(w6).alias("last_known_next_sqft"),
        F.ntile(4).over(w7).alias("inventory_quantity_bucket"),
    )
)

enriched_stores_2 = (
    enriched_stores
    .withColumn("running_count_last_known_next_sqft", F.count("last_known_next_sqft").over(w_ordered))
    .withColumn("running_min_last_known_next_sqft", F.min("last_known_next_sqft").over(w_ordered))
    .withColumn("running_sum_last_known_next_sqft", F.sum("last_known_next_sqft").over(w_ordered))
)

with_last = (
    enriched_stores_2
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.next_sqft",
        "s.next_sqft_2",
        "s.last_known_next_sqft",
        "s.running_count_last_known_next_sqft",
        "s.running_min_last_known_next_sqft",
        F.col("c.amount"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

result = (
    with_last
    .groupBy("region")
    .agg(
        F.percentile_approx("running_count_last_known_next_sqft", [0.25, 0.5, 0.75]).alias("running_count_last_known_next_sqft_quartiles"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.percentile_approx("order_shipping_fee", 0.95).alias("order_shipping_fee_p95"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
)
