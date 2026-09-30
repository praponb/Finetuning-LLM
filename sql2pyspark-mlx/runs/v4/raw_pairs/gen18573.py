from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct")
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy("starts_on", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("channel").orderBy("discount_pct_dense_rank", "id", "promo_id")
w6 = Window.partitionBy("promotion_promo_code").orderBy("promo_id", "id", "promo_id")
w7 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id", "promo_id")
w8 = Window.partitionBy("channel", "promo_code").orderBy("promo_id", "id", "promo_id")

filtered = (
    df_promotions
    .filter(F.col("ends_on") >= F.lit("2023-08-13").cast("date"))
    .select("*", F.initcap(F.trim("promo_code")).alias("promo_code_title"))
)

with_windows = (
    filtered
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("discount_pct_dense_rank", F.dense_rank().over(w2))
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w3))
)

matched = (
    with_windows
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

metrics = (
    matched
    .withColumn("running_event_seq", F.sum("event_seq").over(w4))
    .withColumn("discount_pct_dense_rank_bucket", F.ntile(3).over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("discount_pct_bucket", F.ntile(4).over(w7))
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

attributed = (
    ranked
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

result = (
    attributed
    .select(
        "id",
        "ends_on",
        "discount_pct",
        "promo_code_title",
        "cumulative_discount_pct",
        "promo_id",
        "promotion_promo_code",
        "row_seq",
        "promo_id_2",
    )
    .orderBy("ends_on")
)
