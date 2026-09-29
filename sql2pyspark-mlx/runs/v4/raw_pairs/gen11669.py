from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(((F.col("city") != "New York") & (F.lower(F.trim("supplier_name")) == "tokyo traders")) | (F.length("supplier_name") < 14))
)

suppliers_stats = (
    recent_suppliers
    .groupBy("country")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = suppliers_stats.orderBy("country_values")
