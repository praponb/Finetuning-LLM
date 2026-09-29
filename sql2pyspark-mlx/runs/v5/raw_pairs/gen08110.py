from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "outer")
    .groupBy(F.col("suppliers.country"))
    .agg(
        F.round(F.avg(F.col("products.cost")), 2).alias("avg_cost"),
        F.sort_array(F.collect_set(F.col("products.category"))).alias("category_list"),
    )
    .orderBy(F.col("country").desc())
    .limit(50)
)
