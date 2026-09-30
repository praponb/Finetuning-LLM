from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("row_seq").desc_nulls_first(), "id")
w3 = Window.partitionBy("promo_code").orderBy(F.col("seq_no").desc_nulls_first(), "id")
w4 = Window.partitionBy("promo_code").orderBy(F.col("row_seq").asc_nulls_last(), "id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("starts_on") > F.lit("2023-07-16").cast("date")))
    .select(
        "id",
        "promo_code",
        "starts_on",
        F.initcap(F.trim("channel")).alias("channel_title"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("starts_on") > "2024-09-01")
    .select(
        "id",
        "promo_code",
        F.row_number().over(w_grp).alias("row_seq"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

deduped_promotions = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped_promotions
    .groupBy(
        "seq_no",
        F.when(F.col("row_seq") >= 9, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
    )
    .agg(
        F.count_if(F.col("promo_code") == "WELCOME5").alias("welcome5_events"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .orderBy(F.col("row_seq_tier").asc_nulls_first())
)
