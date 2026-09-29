from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w_grp = Window.partitionBy("promo_code").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code").orderBy(F.col("running_avg_seq_no").desc_nulls_last())
w8 = Window.partitionBy("promo_code").orderBy(F.col("running_min_seq_no_int").desc_nulls_first())

scored = (
    df_promotions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("discount_pct_avg_6_prior", F.avg("discount_pct").over(w2))
    .withColumn("discount_pct_avg_5_prior", F.avg("discount_pct").over(w3))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w4))
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "promo_code",
        "seq_no",
        "discount_pct_avg_6_prior",
        "last_known_channel",
    )
)

scored_promotions = (
    latest
    .select(
        "*",
        F.avg("seq_no").over(w_grp).alias("running_avg_seq_no"),
        F.min("seq_no").over(w_grp).alias("running_min_seq_no"),
        F.count("seq_no").over(w_grp).alias("running_count_seq_no"),
    )
)

latest_promotions = (
    scored_promotions
    .withColumn("latest_rank", F.rank().over(w7))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    latest_promotions
    .filter(F.col("running_count_seq_no").isNotNull())
    .select(
        "*",
        F.least("latest_rank", "discount_pct_avg_6_prior").alias("least_latest_rank_discount_pct_avg_6_prior"),
        F.col("running_min_seq_no").cast("int").alias("running_min_seq_no_int"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "seq_no",
        "discount_pct_avg_6_prior",
        "last_known_channel",
        "running_avg_seq_no",
        "running_min_seq_no",
        "latest_rank",
        "least_latest_rank_discount_pct_avg_6_prior",
        "running_min_seq_no_int",
    )
    .orderBy(F.col("seq_no").desc_nulls_first(), "discount_pct_avg_6_prior")
)
