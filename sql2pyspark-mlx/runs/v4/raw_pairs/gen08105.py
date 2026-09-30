from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w2 = Window.partitionBy("channel", "promo_code")
w3 = Window.orderBy(F.col("discount_pct_share").desc(), "id")
w4 = Window.orderBy("discount_pct_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("promo_code")
w6 = Window.orderBy(F.col("discount_pct").desc(), "id")
w7 = Window.partitionBy("promo_code").orderBy("discount_pct_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_promotions
    .withColumn("discount_pct_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4))
)

scored_promotions = (
    scored
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("first_known_discount_pct_share", F.first("discount_pct_share", ignorenulls=True).over(w4))
    .withColumn("promo_code_max_discount_pct", F.max("discount_pct").over(w5))
    .withColumn("discount_pct_bucket", F.ntile(4).over(w6))
    .withColumn("last_known_channel", F.last("channel", ignorenulls=True).over(w7))
)

with_window = (
    scored_promotions
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window
    .select(
        "id",
        "starts_on",
        "ends_on",
        "discount_pct_percent_rank",
        "discount_pct_share",
        "row_seq",
        "promo_code_max_discount_pct",
        "discount_pct_bucket",
        "promo_id",
    )
)
