from pyspark.sql import functions as F

result = (
    df_products
    .cube(F.month("launch_date").alias("launch_month"), "supplier_id")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.countDistinct("cost").alias("unique_categorys"),
    )
    .orderBy(F.col("supplier_id").desc())
)
