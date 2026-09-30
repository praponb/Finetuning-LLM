from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("launch_date") >= "2024-03-24")
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("price")).otherwise(0)).alias("globex_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(F.col("s.city"), F.col("st.globex_price"))
    .orderBy("city")
)
