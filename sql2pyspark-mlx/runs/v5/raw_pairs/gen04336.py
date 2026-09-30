from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("segment", "customer_id")
w4 = Window.partitionBy("customer_id", "segment").orderBy(F.col("qty_share").asc_nulls_last())
w5 = Window.partitionBy("customer_id").orderBy(F.col("qty_share_2").desc())

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select("id", "customer_id", F.col("line.qty"), F.col("line.price"))
)

joined = (
    exploded
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.age"), F.col("d.segment"))
)

matched_keys = (
    joined
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.col("created_at") < "2025-06-07")
        .select("customer_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "customer_id",
        "qty",
        "price",
        "age",
        "segment",
        F.round((F.col("qty") / F.sum("qty").over(w)), 4).alias("qty_share"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share_2"),
        F.round((F.col("qty") / F.sum("qty").over(w3)), 4).alias("qty_share_3"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "price",
        "age",
        "segment",
        "qty_share",
        "qty_share_2",
        "qty_share_3",
        F.stddev_samp("qty_share_2").over(w2).alias("customer_id_stddev_samp_qty_share_2"),
        F.percent_rank().over(w4).alias("qty_share_percent_rank"),
        F.rank().over(w5).alias("qty_share_2_rank"),
        F.max("age").over(w2).alias("customer_id_max_age"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share_4"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("qty_share_4").between(0.21, 0.68))
    .select(
        "id",
        "price",
        "age",
        "segment",
        "qty_share",
        "customer_id_stddev_samp_qty_share_2",
        "customer_id_max_age",
        "qty_share_4",
        F.round((F.col("qty_share_4") * 1.07), 2).alias("qty_share_4_adjusted"),
        F.when(F.col("customer_id_max_age") >= 65, "High").when(F.col("customer_id_max_age") >= 26, "Medium").otherwise("Low").alias("customer_id_max_age_tier"),
        F.coalesce("segment", F.lit("OTHER")).alias("resolved_segment"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "price",
        "qty_share",
        "customer_id_stddev_samp_qty_share_2",
        "customer_id_max_age",
        "qty_share_4",
        "customer_id_max_age_tier",
        "resolved_segment",
    )
    .orderBy("price")
)
