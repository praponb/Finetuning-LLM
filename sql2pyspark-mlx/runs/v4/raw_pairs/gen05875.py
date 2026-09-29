from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("status").orderBy(F.col("quantity").desc_nulls_first())
w4 = Window.orderBy("quantity", "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2024-02-06"))) & F.col("region").isNotNull() & F.col("opened_date").between("2024-09-14", "2025-03-01"))
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        F.when(F.col("sqft") >= 15462, "High").when(F.col("sqft") >= 4981, "Medium").otherwise("Low").alias("sqft_level"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.shipping_fee").alias("order_shipping_fee"),
        F.col("c.status"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"), F.col("c.quantity"))
)

enriched = (
    with_last
    .select(
        "*",
        F.dense_rank().over(w3).alias("quantity_dense_rank"),
        F.ntile(5).over(w4).alias("quantity_bucket"),
    )
)

result = (
    enriched
    .withColumn("quantity_bucket_tier", F.when(F.col("quantity_bucket") >= 8, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quantity_dense_rank_tier", F.when(F.col("quantity_dense_rank") >= 8, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quantity_bucket_tier", "city", "quantity_dense_rank_tier"], ["quantity_bucket_tier", "city"], ["quantity_bucket_tier"]],
        "quantity_bucket_tier",
        "city",
        "quantity_dense_rank_tier",
    )
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("order_shipping_fee")).otherwise(0)).alias("london_order_shipping_fee"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("sqft"))).alias("returned_total_sqft"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("returned_total_sqft") < 0)
)
