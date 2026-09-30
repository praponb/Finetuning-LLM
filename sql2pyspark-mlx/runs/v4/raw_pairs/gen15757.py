from pyspark.sql import functions as F

result = (
    df_promotions
    .select("id")
    .subtract(
        df_promotions
        .filter(F.col("ends_on") > "2025-05-01")
        .select("id")
    )
)
