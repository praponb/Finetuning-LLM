from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("s.supplier_name"),
        F.col("p.brand"),
        F.col("p.cost"),
        F.col("p.price"),
    )
    .orderBy("brand")
)
