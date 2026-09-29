from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.col("brand").isin(["Globex", "Initech"]))
    .groupBy("supplier_id")
    .agg(
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.avg("cost").alias("avg_cost"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .filter(F.col("st.avg_cost") >= 410)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("st.home_count"),
        F.col("st.avg_cost"),
    )
    .orderBy(F.col("home_count").desc())
)
