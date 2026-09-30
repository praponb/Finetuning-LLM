from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("pos").asc_nulls_last())
w2 = Window.partitionBy("segment", "customer_id")
w3 = Window.partitionBy("customer_id")

exploded = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "elem"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "pos",
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

remaining_keys = (
    exploded
    .select("customer_id", "id")
    .exceptAll(
        df_carts
        .filter(F.to_date("created_at") >= "2024-06-01")
        .select("customer_id", "id")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select("e.customer_id", "e.pos", F.col("b.segment"), F.col("b.age"))
)

with_windows = (
    with_dims
    .filter(F.col("pos").between(1, 3))
    .select(
        "*",
        F.dense_rank().over(w).alias("pos_dense_rank"),
        F.avg("pos").over(w2).alias("segment_avg_pos"),
        F.count("pos").over(w3).alias("customer_id_count_pos"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("pos") >= 3, "PREMIUM").otherwise("STANDARD").alias("pos_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count_2"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)
