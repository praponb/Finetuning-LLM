from pyspark.sql import functions as F

supplier_totals = (
    df_products
    .filter(~F.col("category").isin(["Home", "Sports"]))
    .groupBy("supplier_id")
    .agg(
        F.count("*").alias("cnt"),
        F.min("launch_date").alias("first_launch_date"),
    )
)

result = (
    df_suppliers.alias("s")
    .join(supplier_totals.alias("t"), F.col("s.id") == F.col("t.supplier_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("t.cnt"),
        F.col("t.first_launch_date"),
    )
    .orderBy("city")
)
