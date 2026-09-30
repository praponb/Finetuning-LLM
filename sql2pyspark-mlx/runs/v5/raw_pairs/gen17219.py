from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("discount").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("shipment_carrier", "store_id")
w4 = Window.orderBy("amount", "id")
w5 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)

latest = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest_match = (
    latest
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.amount",
        "s.discount",
        "s.rn",
        F.col("c.carrier").alias("shipment_carrier"),
    )
)

scored = (
    latest_match
    .select(
        "*",
        F.corr("discount", "amount").over(w3).alias("corr_discount_amount"),
        F.ntile(5).over(w4).alias("amount_bucket"),
    )
)

joined = (
    scored
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id").eqNullSafe(F.col("s.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.status",
        "src.amount_bucket",
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
)

metrics = (
    joined
    .filter(F.col("customer_id").isin([1, 9, 26, 31]))
    .withColumn("last7_max_amount_bucket", F.max("amount_bucket").over(w5))
    .withColumn("last7_avg_amount_bucket", F.avg("amount_bucket").over(w5))
    .withColumn("last7_min_amount_bucket", F.min("amount_bucket").over(w5))
)

joined_orders = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.store_id",
        "e.status",
        "e.amount_bucket",
        "e.opened_date",
        "e.last7_max_amount_bucket",
        "e.last7_avg_amount_bucket",
        F.col("b.city"),
    )
)

stats = (
    joined_orders
    .groupBy(
        F.when(F.col("last7_avg_amount_bucket") >= 7.6, "PREMIUM").otherwise("STANDARD").alias("last7_avg_amount_bucket_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.percentile_approx("amount_bucket", 0.9).alias("amount_bucket_p90"),
        F.round(F.sum("last7_avg_amount_bucket") / F.nullif(F.count(F.when(F.col("city") == "Tokyo", 1)), F.lit(0)), 2).alias("last7_avg_amount_bucket_per_tokyo"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("amount_bucket")).otherwise(0)).alias("chiang_mai_amount_bucket"),
        F.round(F.sum("last7_max_amount_bucket") / F.nullif(F.count(F.when(F.col("status") == "COMPLETED", 1)), F.lit(0)), 2).alias("last7_max_amount_bucket_per_completed"),
    )
)

result = (
    stats
    .select(
        "last7_avg_amount_bucket_tier",
        "london_count",
        "amount_bucket_p90",
        "last7_avg_amount_bucket_per_tokyo",
        "london_events",
        "last7_max_amount_bucket_per_completed",
    )
)
