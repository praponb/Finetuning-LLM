from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("brand") == "Acme")
    .groupBy("supplier_id")
    .agg(F.count("*").alias("product_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .filter(F.col("st.product_count") >= 6)
    .select(F.col("s.supplier_name"), F.col("s.country"), F.col("st.product_count"))
    .orderBy("country")
)
