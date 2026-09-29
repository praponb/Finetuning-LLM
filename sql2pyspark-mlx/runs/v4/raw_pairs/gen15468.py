from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.countDistinct("price").alias("unique_categorys"),
        F.max("launch_date").alias("last_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.supplier_name"),
        F.col("st.unique_categorys"),
        F.col("st.last_launch_date"),
    )
)
