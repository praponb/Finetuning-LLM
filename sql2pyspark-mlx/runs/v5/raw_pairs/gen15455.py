from pyspark.sql import functions as F

result = (
    df_promotions
    .select("promo_code", "channel")
    .intersectAll(
        df_promotions
        .filter(F.col("ends_on").between("2024-08-11", "2025-06-19"))
        .select("promo_code", "channel")
    )
)
