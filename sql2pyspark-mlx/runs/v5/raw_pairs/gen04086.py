from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter((F.upper(F.trim("supplier_name")) == "SIAM PARTS CO") & (F.col("city") == "Singapore"))
)

summary = (
    base
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

result = (
    summary
    .filter(F.col("london_count") > 27)
    .orderBy("city", F.col("london_count").desc())
)
