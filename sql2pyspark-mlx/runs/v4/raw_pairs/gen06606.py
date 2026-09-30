from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("units").alias("max_units"),
        F.max("revenue").alias("max_revenue"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("pt"), F.col("p.id") == F.col("pt.product_id"), "inner")
    .filter(F.col("pt.max_units") > 18)
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("pt.max_units"),
        F.col("pt.max_revenue"),
    )
    .orderBy(F.col("supplier_id").desc(), "launch_date", "id")
    .limit(10)
)
