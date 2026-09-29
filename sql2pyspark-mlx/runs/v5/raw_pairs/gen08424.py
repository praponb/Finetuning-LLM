from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.when(F.col("p.brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
        F.sort_array(F.collect_set(F.col("p.brand"))).alias("brand_list"),
    )
    .orderBy(F.col("initech_count").desc(), "country")
)
