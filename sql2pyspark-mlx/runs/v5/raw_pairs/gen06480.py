from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("status").orderBy("sqft", "id")

cleaned = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-08-10").cast("date"))
    .withColumn("city_length", F.length("city"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("DIRECT")))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date").alias("order_order_date"), F.col("c.status"))
)

latest = (
    latest_match
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "store_name",
        "city_length",
        "resolved_region",
        "order_order_date",
        "status",
    )
)

result = (
    latest
    .groupBy("status", F.date_trunc("WEEK", "order_order_date").alias("week_start"))
    .agg(
        F.round(F.sum("city_length") / F.nullif(F.count(F.when(F.col("status") == "SHIPPED", 1)), F.lit(0)), 2).alias("city_length_per_shipped"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("city_length"), 2).alias("avg_city_length"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
