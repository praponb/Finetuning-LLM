from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.lower(F.trim("product_name")) == "yoga mat")
    .groupBy("supplier_id")
    .agg(F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("ss"), F.col("s.id") == F.col("ss.supplier_id"), "left")
    .filter(F.col("ss.home_count") > 78)
    .select(F.col("s.supplier_name"), F.col("s.rating"), F.col("ss.home_count"))
)
