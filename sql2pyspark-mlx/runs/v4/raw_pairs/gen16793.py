from pyspark.sql import functions as F

supplier_stats = (
    df_products
    .filter(F.col("cost").isNull())
    .groupBy("supplier_id")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.countDistinct("cost").alias("unique_categorys"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_stats.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.unique_categorys") > 7)
    .select(
        F.col("s.country"),
        F.col("t.latest_launch_date"),
        F.col("t.unique_categorys"),
    )
)
