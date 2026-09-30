from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promotion_promo_code").orderBy("ends_on")

ranked = (
    df_promotions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

matched = (
    ranked
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("src.ends_on") >= F.col("pr.starts_on")) & (F.col("src.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

deduped = (
    matched
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "BUNDLE"])
    .agg(F.max("discount_pct").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
