from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([4, 7, 8]))
    .select(
        "id",
        "supplier_id",
        "stock_qty",
        "cost",
        F.initcap(F.trim("brand")).alias("brand_title"),
    )
)
