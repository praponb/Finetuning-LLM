from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("st.globex_count"))
    .orderBy(F.col("city").desc())
)
