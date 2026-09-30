from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.lower("product_name").like("%unn%"))
    .select(
        "product_name",
        "launch_date",
        "brand",
        F.floor("cost").alias("cost_whole"),
        F.regexp_replace("product_name", " ", "").alias("product_name_slug"),
    )
)
