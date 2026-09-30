from pyspark.sql import functions as F

result = (
    df_promotions
    .select("id", "promo_code")
    .intersectAll(
        df_promotions
        .filter(~F.col("promo_code").isin(["BUNDLE", "WELCOME5", "VIP"]))
        .select("id", "promo_code")
    )
)
