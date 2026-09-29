from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", 1).otherwise(0)).alias("umbrella_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .filter(F.col("st.umbrella_count") >= 49)
    .select(F.col("s.id"), F.col("s.city"), F.col("st.umbrella_count"))
    .orderBy("umbrella_count")
)
