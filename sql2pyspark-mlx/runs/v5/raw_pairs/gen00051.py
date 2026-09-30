from pyspark.sql import functions as F

result = (
    df_promotions
    .select("id", "promo_code")
    .intersect(
        df_promotions
        .filter(F.col("ends_on") >= "2025-04-01")
        .select("id", "promo_code")
    )
)
