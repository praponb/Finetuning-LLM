from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country"))
    .agg(F.sort_array(F.collect_set(F.col("p.category"))).alias("category_list"))
    .orderBy("country")
)
