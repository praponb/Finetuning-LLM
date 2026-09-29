from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.length("product_name") > 8) & F.month("launch_date").isin([1, 5, 8]))
    .select(
        "id",
        "brand",
        "category",
        "launch_date",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)
