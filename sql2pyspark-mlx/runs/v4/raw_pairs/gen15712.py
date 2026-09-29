from pyspark.sql import functions as F

result = (
    df_promotions
    .select("channel", "id")
    .subtract(
        df_promotions
        .filter(F.col("discount_pct") < 0.28)
        .select("channel", "id")
    )
)
