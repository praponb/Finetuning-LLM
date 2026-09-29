from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel")

enriched = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.lead("discount_pct", 1, 0).over(w).alias("next_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w2)), 4).alias("discount_pct_share"),
    )
)

result = (
    enriched
    .select("id", "channel", "discount_pct")
    .orderBy(F.col("id").asc_nulls_first())
    .limit(10)
)
