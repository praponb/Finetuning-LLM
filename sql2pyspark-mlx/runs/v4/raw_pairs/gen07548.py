from pyspark.sql import functions as F

result = (
    df_products
    .select("brand")
    .intersectAll(
        df_products
        .filter(F.length("product_name") > 6)
        .select("brand")
    )
)
