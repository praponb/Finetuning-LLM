from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .filter(F.col("st.cost_range") >= 58)
    .select(F.col("s.city"), F.col("s.rating"), F.col("st.cost_range"))
    .orderBy(F.col("cost_range").desc())
)
