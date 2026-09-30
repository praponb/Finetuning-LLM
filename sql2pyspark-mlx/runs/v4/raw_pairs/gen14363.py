from pyspark.sql import functions as F

result = (
    df_promotions
    .select("promo_code", "channel")
    .subtract(
        df_promotions
        .filter(F.col("promo_code").isin(["SPRING10", "VIP"]))
        .select("promo_code", "channel")
    )
)
