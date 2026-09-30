from pyspark.sql import functions as F

result = (
    df_promotions
    .select("channel", "id")
    .subtract(
        df_promotions
        .filter(F.col("starts_on").between("2024-08-02", "2024-11-01"))
        .select("channel", "id")
    )
)
