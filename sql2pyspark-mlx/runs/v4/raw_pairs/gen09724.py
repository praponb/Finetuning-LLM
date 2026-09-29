from pyspark.sql import functions as F

result = (
    df_promotions
    .select("id")
    .exceptAll(
        df_promotions
        .filter(F.col("promo_code") != "BUNDLE")
        .select("id")
    )
)
