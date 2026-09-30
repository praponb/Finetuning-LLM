from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sum("cost").alias("total_cost"),
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .filter(F.col("st.home_count") >= 36)
    .select(F.col("s.rating"), F.col("st.total_cost"), F.col("st.home_count"))
    .orderBy("total_cost", F.col("rating").desc())
)
