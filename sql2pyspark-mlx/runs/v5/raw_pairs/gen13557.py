from pyspark.sql import functions as F

result = (
    df_products
    .select("category", "id")
    .intersect(
        df_products
        .filter(F.col("brand") == "Umbrella")
        .select("category", "id")
    )
)
