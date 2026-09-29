from pyspark.sql import functions as F

excluded_suppliers = df_suppliers.filter(F.col("city").isin(["London", "Bangkok"])).select("id")

result = (
    df_products.alias("p")
    .join(excluded_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_anti")
    .filter(F.col("p.supplier_id").isNotNull() | F.lit(excluded_suppliers.isEmpty()))
    .filter(F.month(F.col("p.launch_date")).isin([2, 5, 11]))
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.price"),
    )
)
