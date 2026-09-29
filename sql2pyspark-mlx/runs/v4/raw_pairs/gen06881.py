from pyspark.sql import functions as F

result = (
    df_products
    .select("category", "brand")
    .intersectAll(
        df_products
        .filter(F.month("launch_date").isin([6, 8, 12]))
        .select("category", "brand")
    )
)
