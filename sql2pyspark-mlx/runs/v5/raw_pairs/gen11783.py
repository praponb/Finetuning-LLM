from pyspark.sql import functions as F

supplier_summary = (
    df_products
    .filter(F.col("brand").isin(["Acme", "Umbrella", "Globex"]))
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_summary.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .filter(F.col("t.stddev_price") > 24)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("t.brand_values"),
        F.col("t.stddev_price"),
    )
    .orderBy(F.col("stddev_price").desc(), "id")
    .limit(5)
)
