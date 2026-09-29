from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("sqft").desc(), "id")
w3 = Window.orderBy("order_discount", "id").rowsBetween(-3, -1)
w4 = Window.orderBy("order_discount", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy(F.col("sqft_avg_3").desc(), "id")
w7 = Window.orderBy("sqft_avg_6", "id").rowsBetween(-3, -1)
w8 = Window.orderBy("sqft_per_sqft_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("sqft_avg_3", "id").rowsBetween(-5, Window.currentRow)

cleaned = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
        F.coalesce("region", F.lit("DIRECT")).alias("resolved_region"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.sqft",
        "s.resolved_region",
        F.col("c.discount").alias("order_discount"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id") == 7), "left_anti")
)

filtered = (
    eligible
    .filter(F.lower(F.trim("store_name")) == "downtown express")
    .select(
        "*",
        F.when(F.col("order_discount") >= 0.32, "High").when(F.col("order_discount") >= 0.16, "Medium").otherwise("Low").alias("order_discount_level"),
    )
)

enriched = (
    filtered
    .filter(F.lower(F.trim("store_name")) == "airport kiosk")
    .select(
        "id",
        "sqft",
        "resolved_region",
        "order_discount",
        "order_discount_level",
        F.ntile(5).over(w2).alias("sqft_bucket"),
        F.avg("order_discount").over(w3).alias("order_discount_avg_3_prior"),
        F.avg("sqft").over(w4).alias("sqft_avg_3"),
        F.avg("sqft").over(w5).alias("sqft_avg_6"),
    )
)

filtered_stores = (
    enriched
    .filter(F.col("sqft").isNotNull())
    .select(
        "id",
        "sqft",
        "resolved_region",
        "order_discount",
        "order_discount_level",
        "sqft_bucket",
        "order_discount_avg_3_prior",
        "sqft_avg_3",
        "sqft_avg_6",
        F.round((F.col("sqft") / F.col("sqft_bucket")), 2).alias("sqft_per_sqft_bucket"),
        F.col("order_discount").cast("int").alias("order_discount_int"),
    )
)

eligible_stores = (
    filtered_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.status") == "PENDING"), "left_anti")
)

result = (
    eligible_stores
    .select(
        "id",
        "sqft",
        "resolved_region",
        "sqft_bucket",
        "order_discount_avg_3_prior",
        "sqft_avg_3",
        "sqft_avg_6",
        "sqft_per_sqft_bucket",
        F.ntile(3).over(w6).alias("sqft_avg_3_bucket"),
        F.avg("order_discount_int").over(w7).alias("order_discount_int_avg_3_prior"),
        F.sum("sqft_avg_3").over(w8).alias("running_sqft_avg_3"),
        F.avg("sqft_avg_6").over(w9).alias("sqft_avg_6_avg_5"),
    )
)
