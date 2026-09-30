from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.max("price").alias("max_price"),
        F.countDistinct("stock_qty").alias("unique_categorys"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("st.max_price"),
        F.col("st.unique_categorys"),
    )
    .orderBy(F.col("city").desc(), "unique_categorys")
)
