from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .groupBy("supplier_id")
    .agg(
        F.min("stock_qty").alias("min_stock_qty"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .filter(F.col("st.min_stock_qty") > 280)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.supplier_name"),
        F.col("st.min_stock_qty"),
        F.col("st.category_values"),
    )
    .orderBy(F.col("min_stock_qty").desc(), "supplier_name")
)
