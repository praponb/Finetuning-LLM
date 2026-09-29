from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("product_count"),
        F.countDistinct("stock_qty").alias("unique_categorys"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.rating"),
        F.col("st.product_count"),
        F.col("st.unique_categorys"),
    )
)
