from pyspark.sql import functions as F

result = (
    df_promotions
    .select("channel")
    .subtract(
        df_promotions
        .filter(F.col("starts_on") <= "2024-08-01")
        .select("channel")
    )
)
