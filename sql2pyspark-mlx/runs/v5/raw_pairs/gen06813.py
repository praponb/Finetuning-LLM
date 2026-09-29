from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city", "region").orderBy(F.col("store_sale_revenue").asc_nulls_last(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.revenue").alias("store_sale_revenue"),
        F.col("c.units").alias("store_sale_units"),
    )
)

latest_match_stores = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount").alias("order_discount"))
)

latest = (
    latest_match_stores
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"), F.col("c.amount"))
)

result = (
    with_last
    .withColumn("shipping_fee_tier", F.when(F.col("shipping_fee") >= 36, "HIGH").otherwise("LOW"))
    .cube("shipping_fee_tier", "city")
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("amount_per_berlin"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.percentile_approx("store_sale_revenue", 0.9).alias("store_sale_revenue_p90"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("store_sale_units")).otherwise(0)).alias("bangkok_store_sale_units"),
        F.grouping("shipping_fee_tier").alias("grouping_id"),
    )
)
