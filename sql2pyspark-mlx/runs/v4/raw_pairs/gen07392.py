from pyspark.sql import functions as F

result = (
    df_promotions
    .select("id", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("ends_on") >= "2025-07-25")
        .select("id", "promo_code")
    )
)
