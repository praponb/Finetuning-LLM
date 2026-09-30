from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "status").orderBy(F.col("is_partner").desc_nulls_first(), "id")
w2 = Window.partitionBy("channel", "status").orderBy("dedup_rank")
w_running = Window.partitionBy("status").orderBy(F.col("discount").desc(), "id")
w4 = Window.partitionBy("status").orderBy("amount", "id")
w5 = Window.partitionBy("status").orderBy(F.col("row_seq").asc_nulls_first())
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("status").orderBy(F.col("amount_bucket").desc_nulls_last())
w8 = Window.orderBy(F.col("amount_bucket_share").asc_nulls_first())
w9 = Window.orderBy(F.col("amount_bucket_percent_rank").desc_nulls_last())
w10 = Window.orderBy(F.col("amount_bucket_share").desc_nulls_last())
w11 = Window.partitionBy("amount_bucket_share_rank_2_tier").orderBy(F.col("avg_amount_bucket_share").desc())

valid = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(
        "id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("channel") == "partner", 1).otherwise(0).alias("is_partner"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    ranked
    .withColumn("dedup_rank_2", F.rank().over(w2))
    .filter(F.col("dedup_rank_2") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "status",
        F.row_number().over(w_running).alias("row_seq"),
        F.ntile(4).over(w4).alias("amount_bucket"),
    )
)

kept_keys = (
    metrics
    .select("id", "status")
    .subtract(
        df_orders
        .filter(F.col("shipping_fee") >= 20)
        .select("id", "status")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "id",
        "row_seq",
        "amount_bucket",
        F.dense_rank().over(w5).alias("row_seq_dense_rank"),
        F.round((F.col("amount_bucket") / F.sum("amount_bucket").over(w6)), 4).alias("amount_bucket_share"),
        F.percent_rank().over(w7).alias("amount_bucket_percent_rank"),
    )
)

enriched_orders = (
    enriched
    .select(
        "row_seq",
        "amount_bucket",
        "row_seq_dense_rank",
        "amount_bucket_share",
        "amount_bucket_percent_rank",
        F.rank().over(w8).alias("amount_bucket_share_rank"),
        F.cume_dist().over(w9).alias("amount_bucket_percent_rank_cume_dist"),
        F.rank().over(w10).alias("amount_bucket_share_rank_2"),
    )
)

result = (
    enriched_orders
    .groupBy(
        F.when(F.col("amount_bucket_share_rank_2") >= 9, "LARGE").otherwise("SMALL").alias("amount_bucket_share_rank_2_tier"),
        F.when(F.col("row_seq_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("row_seq_dense_rank_tier"),
    )
    .agg(
        F.avg("amount_bucket_share_rank_2").alias("avg_amount_bucket_share_rank_2"),
        F.round(F.avg("amount_bucket_share"), 2).alias("avg_amount_bucket_share"),
        F.sum("amount_bucket_percent_rank").alias("total_amount_bucket_percent_rank"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.max("amount_bucket_share_rank").alias("max_amount_bucket_share_rank"),
    )
    .withColumn("avg_amount_bucket_share_percent_rank", F.percent_rank().over(w11))
)
