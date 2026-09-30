from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("shipping_fee", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("order_date", "id")

valid = (
    df_orders
    .filter((F.col("channel").eqNullSafe(F.lit("store"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.concat("channel", F.lit(" - "), "status").alias("channel_status"),
        F.round((F.col("amount") * 100), 2).alias("adjusted_amount"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "store_id",
        "order_date",
        "discount",
        "shipping_fee",
        "adjusted_amount",
    )
)

kept = (
    ranked
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") >= F.col("s.order_date")), "left_anti")
)

scored = (
    kept
    .withColumn("discount_avg_3", F.avg("discount").over(w2))
    .withColumn("prev_discount", F.lag("discount", 2).over(w3))
)

enriched = (
    scored
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.discount",
        "src.shipping_fee",
        "src.discount_avg_3",
        F.col("b.region"),
        F.col("b.sqft"),
    )
)

kept_orders = (
    enriched
    .alias("s")
    .join(df_shipments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.order_id").isin([7, 18, 19])), "left_anti")
)

result = (
    kept_orders
    .groupBy(
        "store_id",
        F.when(F.col("sqft") >= 13128, "LARGE").otherwise("SMALL").alias("sqft_tier"),
    )
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.percentile_approx("discount_avg_3", 0.5).alias("discount_avg_3_p50"),
    )
    .orderBy(F.col("sqft_tier").asc_nulls_last())
)
