from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id")
w3 = Window.partitionBy("promo_code")
w4 = Window.partitionBy("channel", "promo_code").orderBy("next_discount_pct", "id")
w5 = Window.orderBy(F.col("seq_no").desc(), "id")
w6 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("promo_code").orderBy("next_discount_pct", "id")

base = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-10-10").cast("date"))
    .withColumn("clean_channel", F.trim("channel"))
)

metrics = (
    base
    .select(
        "*",
        F.lead("discount_pct", 2).over(w).alias("next_discount_pct"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

enriched = (
    metrics
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
    .withColumn("next_discount_pct_bucket", F.ntile(5).over(w4))
    .withColumn("seq_no_2", F.row_number().over(w5))
    .withColumn("running_next_discount_pct", F.sum("next_discount_pct").over(w6))
    .withColumn("seq_no_3", F.row_number().over(w7))
)

matched = (
    enriched
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        "clean_channel",
        "next_discount_pct",
        "seq_no",
        "discount_pct_share",
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
