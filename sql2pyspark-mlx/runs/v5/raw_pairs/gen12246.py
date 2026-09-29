from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.globex_count") >= 42)
    .select(F.col("s.id"), F.col("s.country"), F.col("st.globex_count"))
    .orderBy("country", "id")
    .limit(3)
)
