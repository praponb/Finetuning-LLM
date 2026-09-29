from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct_whole").desc())

base = (
    df_promotions
    .filter(~F.col("promo_code").isin(["VIP", "BUNDLE", "WELCOME5"]))
    .select(
        "*",
        F.ceil("discount_pct").alias("discount_pct_whole"),
        F.when(F.col("promo_code").isin(["WELCOME5", "VIP"]), "Priority").otherwise("Other").alias("promo_code_group"),
    )
)

result = (
    base
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        F.lead("discount_pct").over(w).alias("next_discount_pct"),
        F.lag("discount_pct").over(w).alias("prev_discount_pct"),
        F.lag("discount_pct_whole").over(w).alias("prev_discount_pct_whole"),
        F.row_number().over(w).alias("row_seq"),
        F.cume_dist().over(w2).alias("discount_pct_whole_cume_dist"),
    )
)
