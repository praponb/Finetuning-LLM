from pyspark.sql import functions as F

result = (
    df_promotions
    .select("channel", "id")
    .intersectAll(
        df_promotions
        .filter(F.year("ends_on") >= 2025)
        .select("channel", "id")
    )
)
