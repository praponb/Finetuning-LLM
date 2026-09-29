from pyspark.sql import functions as F

result = (
    df_promotions
    .select("channel", "promo_code")
    .subtract(
        df_promotions
        .filter(F.col("ends_on") >= "2023-07-30")
        .select("channel", "promo_code")
    )
)
