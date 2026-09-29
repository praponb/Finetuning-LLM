from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "region").orderBy(F.col("order_date").desc(), "id")
w3 = Window.partitionBy("city").orderBy(F.col("opened_date_minus_37d").desc_nulls_first(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

cleaned = (
    df_stores
    .filter(F.col("store_name").like("%River%"))
    .select(
        "*",
        F.concat_ws("/", "city", "store_name").alias("city_store_name_key"),
        F.date_sub("opened_date", 37).alias("opened_date_minus_37d"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"), F.col("c.order_date"))
)

latest = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_last_stores = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"), F.col("c.sale_date"))
)

not_flagged = (
    with_last_stores
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([2, 3, 6])), "left_semi")
)

base = (
    not_flagged
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "sqft",
        "city_store_name_key",
        "opened_date_minus_37d",
        "order_date",
        "latest_rank",
        "revenue",
        (F.col("latest_rank") / F.nullif("shipping_fee", F.lit(0))).alias("latest_rank_per_shipping_fee"),
    )
)

result = (
    base
    .select(
        "id",
        "city",
        "revenue",
        F.col("latest_rank_per_shipping_fee").alias("LATEST_RANK_PER_SHIPPING_FEE"),
    )
    .unpivot(
        ["id", "city"],
        ["revenue", "LATEST_RANK_PER_SHIPPING_FEE"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
