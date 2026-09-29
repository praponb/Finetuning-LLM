from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id") == 37)
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0))) / F.count("*")), 1).alias("pct_initech"),
    )
    .orderBy(F.col("pct_initech").desc(), F.col("brand_values").desc())
)
