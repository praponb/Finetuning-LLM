from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.supplier_id").isNull() & (F.length(F.col("s.supplier_name")) >= 12))
    .select(
        F.col("s.country"),
        F.col("s.city"),
        F.col("s.rating"),
        F.col("p.launch_date"),
        F.col("p.category"),
        F.round((F.col("s.rating") * 1.07), 2).alias("rating_scaled"),
    )
    .orderBy("category", "country")
)
