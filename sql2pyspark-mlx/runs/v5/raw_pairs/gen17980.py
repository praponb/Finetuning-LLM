from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("region", "city").orderBy("opened_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first())
w_recent = Window.partitionBy("city", "region").orderBy("running_sum_order_amount", "id")

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-07-25"))) & (~F.col("region").eqNullSafe(F.lit("West"))))
    .withColumn("opened_date_period", F.date_trunc("YEAR", "opened_date"))
    .withColumn("store_name_lower", F.lower("store_name"))
    .withColumn("region_filled", F.coalesce("region", F.lit("OTHER")))
)

with_latest = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date_period")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.region",
        "s.opened_date",
        "s.sqft",
        "s.opened_date_period",
        "s.store_name_lower",
        F.col("c.amount").alias("order_amount"),
        F.col("c.discount").alias("order_discount"),
    )
)

with_windows = (
    with_latest
    .withColumn("running_min_order_amount", F.min("order_amount").over(w_ordered))
    .withColumn("running_max_order_amount", F.max("order_amount").over(w_ordered))
    .withColumn("running_sum_order_amount", F.sum("order_amount").over(w_ordered))
    .withColumn("sqft_cume_dist", F.cume_dist().over(w3))
)

with_windows_stores = (
    with_windows
    .select(
        "id",
        "opened_date",
        "sqft",
        "opened_date_period",
        "order_amount",
        "running_min_order_amount",
        "running_sum_order_amount",
        "sqft_cume_dist",
        F.lag("order_discount").over(w_recent).alias("prev_order_discount"),
        F.lead("order_discount").over(w_recent).alias("next_order_discount"),
        F.lag("sqft_cume_dist").over(w_recent).alias("prev_sqft_cume_dist"),
        F.lead("sqft_cume_dist").over(w_recent).alias("next_sqft_cume_dist"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

cleaned = (
    with_windows_stores
    .filter(F.col("opened_date") >= F.lit("2023-06-23").cast("date"))
    .select(
        "id",
        "opened_date",
        "sqft",
        "opened_date_period",
        "order_amount",
        "running_sum_order_amount",
        "sqft_cume_dist",
        "prev_order_discount",
        "next_order_discount",
        "prev_sqft_cume_dist",
        "event_seq",
        F.round((F.col("next_order_discount") * 1.1), 2).alias("adjusted_next_order_discount"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("sqft") >= 17023, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("opened_date_period")) - F.unix_timestamp(F.min("opened_date_period"))) / 60).alias("span_minutes"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("span_minutes") < 1000000)
    .orderBy(F.col("distinct_ids").desc())
)
