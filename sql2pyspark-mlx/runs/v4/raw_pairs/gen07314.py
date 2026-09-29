from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Acme"]))
    .groupBy("supplier_id")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    df_suppliers.alias("suppliers")
    .join(supplier_summary.alias("t"), F.col("suppliers.id") == F.col("t.supplier_id"), "left")
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.rating"),
        F.col("suppliers.country"),
        F.col("t.stddev_cost"),
    )
    .orderBy("rating")
)
