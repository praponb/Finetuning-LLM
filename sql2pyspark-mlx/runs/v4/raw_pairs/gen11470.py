from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.month(F.col("p.launch_date")).isin([1, 5, 9]))
    .select(
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
    )
    .orderBy(F.col("brand").desc())
)
