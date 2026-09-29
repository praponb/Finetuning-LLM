from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0))) / F.count("*")), 1).alias("pct_initech"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("st.pct_initech"),
        F.col("st.total_count"),
    )
)
