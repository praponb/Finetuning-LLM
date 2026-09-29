from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("region").orderBy(F.col("amount").desc(), "id")
w3 = Window.orderBy("order_shipping_fee", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("order_shipping_fee", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("region").orderBy("order_shipping_fee", "id")
w6 = Window.partitionBy("region").orderBy(F.col("event_seq").desc(), "id")
w7 = Window.partitionBy("region").orderBy("event_seq", "id")
w_grp = Window.orderBy("sqft", "id")
w_recent = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w10 = Window.partitionBy("region")
w11 = Window.partitionBy("region").orderBy("prev_amount", "id").rowsBetween(-6, -1)
w12 = Window.partitionBy("region").orderBy("sqft_avg_6_prior", "id")
w13 = Window.orderBy(F.col("amount").desc(), "id")
w14 = Window.partitionBy("region").orderBy("next_amount", "id")
w_ordered = Window.partitionBy("region").orderBy(F.col("seq_no").desc_nulls_last())

latest_match = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount"),
        F.col("c.shipping_fee").alias("order_shipping_fee"),
    )
)

cleaned = (
    latest_match
    .filter((F.col("opened_date") >= F.lit("2024-03-05").cast("date")) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "amount",
        "order_shipping_fee",
        F.concat("region", F.lit("/"), "store_name").alias("region_store_name"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("region_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.row_number().over(w_hist).alias("event_seq"),
        F.avg("sqft").over(w3).alias("sqft_avg_2"),
        F.avg("sqft").over(w4).alias("sqft_avg_6_prior"),
        F.lead("amount", 2).over(w5).alias("next_amount"),
    )
)

with_windows_stores = (
    with_windows
    .select(
        "*",
        F.ntile(5).over(w6).alias("event_seq_bucket"),
        F.ntile(5).over(w7).alias("event_seq_bucket_2"),
        F.lag("amount", 2).over(w_grp).alias("prev_amount"),
        F.ntile(4).over(w_recent).alias("sqft_bucket"),
    )
)

enriched = (
    with_windows_stores
    .withColumn("event_seq_bucket_share", F.round((F.col("event_seq_bucket") / F.sum("event_seq_bucket").over(w10)), 4))
    .withColumn("sqft_avg_6_prior_avg_6_prior", F.avg("sqft_avg_6_prior").over(w11))
    .withColumn("seq_no", F.row_number().over(w12))
    .withColumn("seq_no_2", F.row_number().over(w13))
    .withColumn("next_next_amount", F.lead("next_amount", 1).over(w14))
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w_recent))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.count("prev_amount").over(w10).alias("region_count_prev_amount"),
        F.round((F.col("seq_no_2") / F.sum("seq_no_2").over(w10)), 4).alias("seq_no_2_share"),
        F.dense_rank().over(w_ordered).alias("seq_no_dense_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "order_shipping_fee",
        "region_store_name",
        "region_clean",
        "event_seq",
        "next_amount",
        F.when(F.col("region").rlike("^(North|Central)$"), "PRIORITY").when(F.col("region").rlike("^(South|East)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("region_zone"),
    )
    .orderBy("event_seq")
)
