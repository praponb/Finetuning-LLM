from pyspark.sql import functions as F

products_by_supplier = (
    df_products
    .filter(F.col("cost") < 240)
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fashion"),
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
)

result = (
    df_suppliers.alias("suppliers")
    .join(products_by_supplier.alias("products_by_supplier"), F.col("suppliers.id") == F.col("products_by_supplier.supplier_id"), "inner")
    .select(
        F.col("suppliers.supplier_name"),
        F.col("suppliers.rating"),
        F.col("products_by_supplier.pct_fashion"),
        F.col("products_by_supplier.umbrella_count"),
    )
    .orderBy("pct_fashion", "supplier_name")
)
