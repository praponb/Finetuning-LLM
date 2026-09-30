from pyspark.sql import functions as F

recent_products = df_products.filter((F.year("launch_date") == 2023) | (F.col("supplier_id") == 4))

brand_group_summary = (
    recent_products
    .groupBy(
        F.when(F.col("brand").isin(["Stark", "Umbrella"]), "core").otherwise("other").alias("brand_group"),
        F.year("launch_date").alias("launch_year"),
    )
    .agg(F.countDistinct("stock_qty").alias("unique_categorys"))
)

result = (
    brand_group_summary
    .filter(F.col("unique_categorys") < 7)
    .select("brand_group", "launch_year", "unique_categorys")
    .orderBy(F.col("unique_categorys").desc(), "brand_group")
)
