from pyspark.sql import functions as F

result = (
    df_products
    .select("supplier_id")
    .subtract(
        df_products
        .filter(F.array_contains("tags", "new"))
        .select("supplier_id")
    )
)
