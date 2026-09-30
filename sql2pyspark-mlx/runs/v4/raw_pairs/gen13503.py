from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code")
w2 = Window.orderBy("ends_on", "id").rowsBetween(-6, -1)

enriched = (
    df_promotions
    .select(
        "*",
        F.min("discount_pct").over(w).alias("promo_code_min_discount_pct"),
        F.avg("discount_pct").over(w2).alias("discount_pct_avg_6_prior"),
    )
)

result = (
    enriched
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "discount_pct_avg_6_prior",
        F.when(F.col("promo_code").rlike("^(WELCOME5|VIP)$"), "GROUP_A").otherwise("PRIORITY").alias("promo_code_zone"),
    )
)
