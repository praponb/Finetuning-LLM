from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("order_date", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("discount").desc(), "id")
w_running = Window.orderBy(F.col("shipping_fee").desc_nulls_first())
w4 = Window.partitionBy("channel", "store_id").orderBy(F.col("discount_bucket").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("prev_shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel").orderBy("shipping_fee_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("discount").desc())
w8 = Window.orderBy(F.col("shipping_fee_cume_dist").desc(), "id")
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

cleaned = (
    df_orders
    .filter(F.col("store_id").isNotNull() & F.col("customer_id").isin([13, 16, 17, 20]))
    .select(
        "*",
        F.lower("channel").alias("channel_lower"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_clean"),
    )
)

cleaned_orders = (
    cleaned
    .filter((F.col("order_date") > F.lit("2023-05-07").cast("date")) & F.col("store_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        "channel_lower",
        F.when(F.col("shipping_fee") >= 31, "High").when(F.col("shipping_fee") >= 9, "Medium").otherwise("Low").alias("shipping_fee_tier"),
        F.trim("status").alias("clean_status"),
    )
)

metrics = (
    cleaned_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        "discount",
        "shipping_fee",
        "shipping_fee_tier",
        F.lag("shipping_fee", 1).over(w).alias("prev_shipping_fee"),
        F.ntile(3).over(w2).alias("discount_bucket"),
        F.cume_dist().over(w_running).alias("shipping_fee_cume_dist"),
    )
)

joined = (
    metrics
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .select("a.*", F.col("p.name"))
)

scored = (
    joined
    .filter(F.col("shipping_fee") > 17)
    .select(
        "id",
        "customer_id",
        "shipping_fee_tier",
        "prev_shipping_fee",
        F.row_number().over(w4).alias("event_seq"),
        F.sum("shipping_fee").over(w5).alias("cumulative_shipping_fee"),
        F.first("shipping_fee", ignorenulls=True).over(w6).alias("first_known_shipping_fee"),
        F.dense_rank().over(w7).alias("discount_dense_rank"),
        F.ntile(4).over(w8).alias("shipping_fee_cume_dist_bucket"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipped_date"))
)

joined_orders = (
    latest_match
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.shipping_fee_tier",
        "s.event_seq",
        "s.first_known_shipping_fee",
        "s.shipping_fee_cume_dist_bucket",
        F.col("b.credit_limit"),
        F.col("b.city"),
        F.col("b.signup_date"),
    )
)

result = (
    joined_orders
    .groupingSets([["customer_id", "city"], ["customer_id"], []], "customer_id", "city")
    .agg(
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("event_seq_per_bangkok"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("event_seq"))).alias("tokyo_total_event_seq"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.countDistinct("city").alias("distinct_cities"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("distinct_cities")
)
