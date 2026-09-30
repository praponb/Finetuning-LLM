from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel")
w2 = Window.orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("promo_code")

kept_keys = (
    df_promotions
    .select("promo_code", "id")
    .exceptAll(
        df_promotions
        .filter(F.col("ends_on").between("2023-04-25", "2025-03-13"))
        .select("promo_code", "id")
    )
)

verified = (
    df_promotions
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.promo_code") == F.col("k.promo_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("discount_pct").between(0.13, 0.26))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("unassigned")))
)

enriched = (
    base
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.dense_rank().over(w2).alias("discount_pct_dense_rank"),
        F.avg("discount_pct").over(w3).alias("promo_code_avg_discount_pct"),
    )
)

pivoted = (
    enriched
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "WELCOME5"])
    .agg(F.sum("discount_pct").alias("amount"))
)

result = pivoted.select("channel")
