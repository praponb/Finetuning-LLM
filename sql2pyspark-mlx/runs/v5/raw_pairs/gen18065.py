from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.min("launch_date").alias("min_launch_date"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .select(F.col("s.rating"), F.col("s.city"), F.col("st.min_launch_date"))
    .orderBy("rating")
)
