from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("starts_on_year").asc_nulls_first())
w_grp = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy("starts_on", "id")
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w5 = Window.partitionBy("promo_code").orderBy(F.col("promotion_discount_pct").desc_nulls_first(), "id", "promo_id")

filtered = (
    df_promotions
    .filter(F.month("starts_on").isin([5, 6, 11]))
    .select("*", F.year("starts_on").alias("starts_on_year"))
)

enriched = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("starts_on_year_dense_rank"),
        F.sum("starts_on_year").over(w_grp).alias("running_starts_on_year"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_window = (
    deduped
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & F.col("e.starts_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

filtered_promotions = (
    with_window
    .filter((F.col("starts_on") >= F.lit("2023-04-11").cast("date")) & F.col("channel").isNotNull())
    .withColumn("ends_on_label", F.date_format("ends_on", "yyyy-MM"))
    .withColumn("starts_on_year_dense_rank_plus_seq_no", (F.col("starts_on_year_dense_rank") + F.col("seq_no")))
)

ranked = (
    filtered_promotions
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "starts_on_year",
        "starts_on_year_dense_rank",
        "running_starts_on_year",
        "promo_id",
        "promotion_discount_pct",
        F.when(F.col("promo_code").rlike("^(BUNDLE|WELCOME5)$"), "EXTENDED").when(F.col("promo_code").rlike("^(SPRING10|VIP)$"), "CORE").otherwise("GROUP_A").alias("promo_code_zone"),
    )
    .orderBy(F.col("starts_on").asc_nulls_first())
)
