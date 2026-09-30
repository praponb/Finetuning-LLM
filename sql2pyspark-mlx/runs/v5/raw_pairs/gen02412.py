from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 340)
    .select("id", "brand", "launch_date")
    .intersect(
        df_products
        .filter(F.col("price") <= 1060)
        .select("id", "brand", "launch_date")
    )
)
