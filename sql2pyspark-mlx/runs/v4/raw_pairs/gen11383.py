from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city", "region").orderBy(F.col("quantity").asc_nulls_first())
w4 = Window.partitionBy("city").orderBy("quantity", "id")

filtered = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        F.concat_ws(",", "store_name", "region").alias("store_name_region_key"),
        F.upper("region").alias("region_upper"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date"))
)

with_latest = (
    with_last
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity"))
)

enriched = (
    with_latest
    .select(
        "*",
        F.percent_rank().over(w3).alias("quantity_percent_rank"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

result = (
    enriched
    .groupBy("region")
    .agg(
        F.percentile_approx("quantity_percent_rank", 0.9).alias("quantity_percent_rank_p90"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
)
