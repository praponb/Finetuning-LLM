from pyspark.sql import functions as F

result = (
    df_products
    .select("id")
    .intersect(
        df_products
        .filter(F.col("supplier_id").isNotNull())
        .select("id")
    )
)
