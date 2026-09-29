from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "status").orderBy("discount", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("status", "customer_id").orderBy("discount_avg_3", "id")
w4 = Window.partitionBy("customer_id", "status").orderBy(F.col("shipping_fee").asc_nulls_last())
w5 = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc(), "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

base = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-05-14").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "discount",
        "shipping_fee",
        F.concat_ws("-", "channel", "status").alias("channel_status_key"),
    )
)

scored = (
    base
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "shipping_fee",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.avg("discount").over(w2).alias("discount_avg_3"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped = (
    latest
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

deduped_orders = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_latest = (
    deduped_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipped_date"))
)

base_orders = (
    with_latest
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.initcap(F.trim("status")).alias("status_title"))
)

daily = (
    base_orders
    .cube(F.date_trunc("QUARTER", "shipped_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("status").alias("distinct_status"),
    )
)

result = (
    daily
    .select("quarter_start", "row_count", "num_status")
    .orderBy("num_status")
)
