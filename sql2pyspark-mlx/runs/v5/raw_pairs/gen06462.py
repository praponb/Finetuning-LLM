from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")
w2 = Window.orderBy("city_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city").orderBy(F.col("city_length").desc_nulls_last())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("store_name").isNotNull())
    .withColumn("city_length", F.length("city"))
    .withColumn("region_store_name_key", F.concat_ws("/", "region", "store_name"))
)

enriched = (
    filtered
    .filter(F.col("region") == "West")
    .select(
        "id",
        "store_name",
        "city",
        "city_length",
        F.row_number().over(w).alias("seq_no"),
        F.sum("city_length").over(w2).alias("cumulative_city_length"),
        F.dense_rank().over(w_grp).alias("city_length_dense_rank"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.shipping_fee") <= 38), "left_anti")
)

with_latest = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.amount"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units") > 21), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "city_length",
        "seq_no",
        "cumulative_city_length",
        "city_length_dense_rank",
    )
)
