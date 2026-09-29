from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("price"), 2).alias("stddev_price"))
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("st"), F.col("s.id") == F.col("st.supplier_id"), "left")
    .select(F.col("s.country"), F.col("s.supplier_name"), F.col("st.stddev_price"))
    .orderBy("stddev_price")
)
