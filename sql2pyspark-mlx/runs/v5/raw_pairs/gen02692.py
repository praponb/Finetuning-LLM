from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(((F.col("city") == "New York") & (F.col("city") == "New York")) | (F.col("city") == "Singapore"))
)

country_group_summary = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("country").isin(["JP", "UK"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("country"))).alias("country_values"),
    )
)

result = country_group_summary.orderBy("country_group")
