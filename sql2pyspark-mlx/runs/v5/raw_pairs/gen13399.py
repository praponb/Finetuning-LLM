from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("city").isin(["Bangkok", "Chiang Mai"]))
    .select(
        "*",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.date_trunc("WEEK", "opened_date").alias("opened_date_period"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.opened_date_period")), "left_anti")
)

with_windows = (
    kept
    .filter(F.col("city") == "London")
    .withColumn("sqft_bucket", F.ntile(4).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w3))
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"))
)

result = (
    latest_match
    .groupBy("city")
    .pivot("region", ["East", "South"])
    .agg(F.sum("sqft").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
