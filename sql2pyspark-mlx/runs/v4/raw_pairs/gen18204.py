from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_recent = Window.partitionBy("customer_id", "tag").orderBy(F.col("age").desc_nulls_last())
w_ordered = Window.partitionBy("customer_id")
w4 = Window.partitionBy("segment")
w5 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("customer_id", "tag").orderBy(F.col("customer_id_avg_age").asc_nulls_first())
w_running = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w_grp = Window.partitionBy("city", "segment").orderBy(F.col("age_share").asc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

enriched = (
    exploded
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.segment"), F.col("p.age"))
)

metrics = (
    enriched
    .select(
        "*",
        F.sum("age").over(w).alias("age_sum_1h"),
        F.dense_rank().over(w_recent).alias("age_dense_rank"),
        F.avg("age").over(w_ordered).alias("customer_id_avg_age"),
        F.round((F.col("age") / F.sum("age").over(w4)), 4).alias("age_share"),
        F.count("age").over(w5).alias("age_count_30m"),
    )
)

joined = (
    metrics
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.city"))
)

with_windows = (
    joined
    .withColumn("customer_id_avg_age_share", F.round((F.col("customer_id_avg_age") / F.sum("customer_id_avg_age").over(w6)), 4))
    .withColumn("customer_id_avg_age_rank", F.rank().over(w7))
    .withColumn("customer_id_max_age_count_30m", F.max("age_count_30m").over(w_ordered))
    .withColumn("age_count_30m_max_2h", F.max("age_count_30m").over(w_running))
    .withColumn("age_share_percent_rank", F.percent_rank().over(w_grp))
)

remaining_keys = (
    with_windows
    .select("id", "customer_id")
    .intersect(
        df_carts
        .filter(F.hour("created_at").between(9, 14))
        .select("id", "customer_id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

by_type = (
    kept_rows
    .groupBy("tag", "customer_id")
    .pivot("segment", ["SMB", "Consumer"])
    .agg(F.sum("customer_id_avg_age").alias("amount"))
)

result = (
    by_type
    .select(
        "tag",
        "customer_id",
        "SMB",
        "Consumer",
        F.when(F.col("tag").rlike("^(VIP|WELCOME5)$"), "GROUP_A").when(F.col("tag").rlike("^(SPRING10|BUNDLE)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("tag_zone"),
    )
)
