from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("discount").asc_nulls_last())
w_recent = Window.partitionBy("store_id").orderBy(F.col("discount").asc_nulls_last())
w3 = Window.partitionBy("store_id")

base = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-08-21").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.sqft"))
)

matched_keys = (
    enriched
    .select("status", "id")
    .intersect(
        df_orders
        .filter(F.col("discount") <= 0.29)
        .select("status", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_orders = (
    verified
    .withColumn("discount_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("discount_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("store_id_avg_discount", F.avg("discount").over(w3))
)

result = (
    enriched_orders
    .select(
        "status",
        "amount",
        "discount",
        "status_prefix",
        "sqft",
        "discount_dense_rank",
        "store_id_avg_discount",
        F.when(F.col("status").rlike("^(PENDING|SHIPPED)$"), "GROUP_A").when(F.col("status").rlike("^(COMPLETED|CANCELLED)$"), "OTHER_ZONE").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("store_id_avg_discount").desc_nulls_first())
)
