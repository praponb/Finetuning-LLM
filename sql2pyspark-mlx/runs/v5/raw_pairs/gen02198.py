from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

scored = (
    df_stores
    .filter(F.col("city") != "Bangkok")
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("running_sqft", F.sum("sqft").over(w2))
    .withColumn("sqft_avg_3", F.avg("sqft").over(w3))
)

with_last = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.opened_date",
        "s.sqft_share",
        "s.sqft_avg_3",
        F.col("c.discount"),
        F.col("c.shipping_fee"),
    )
)

cleaned = (
    with_last
    .filter(F.col("city") != "Chiang Mai")
    .withColumn("least_sqft_share_discount", F.least("sqft_share", "discount"))
)

with_last_stores = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.sqft_share",
        "s.shipping_fee",
        "s.least_sqft_share_discount",
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

result = (
    with_last_stores
    .groupBy(
        F.when(F.col("inventory_quantity") >= 214, "HIGH").otherwise("LOW").alias("inventory_quantity_tier"),
    )
    .agg(
        F.percentile_approx("least_sqft_share_discount", 0.9).alias("least_sqft_share_discount_p90"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("least_sqft_share_discount"))).alias("chiang_mai_total_least_sqft_share_discount"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("inventory_quantity_tier").asc_nulls_first())
)
