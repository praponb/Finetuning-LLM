from pyspark.sql import functions as F

result = (
    df_products
    .select("category")
    .intersectAll(
        df_products
        .filter(F.col("supplier_id").isin([12, 17, 30]))
        .select("category")
    )
)
