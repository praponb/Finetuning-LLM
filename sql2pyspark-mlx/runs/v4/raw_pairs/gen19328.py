from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").isNotNull())
    .select("id", "launch_date")
    .intersect(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("id", "launch_date")
    )
)
