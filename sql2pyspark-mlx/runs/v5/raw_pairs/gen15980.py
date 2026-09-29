from pyspark.sql import functions as F

not_flagged = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (~F.col("ch.product_name").like("%wirele%")), "left_anti")
)

result = (
    not_flagged
    .groupBy("city")
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("singapore_events") >= 1)
    .orderBy(F.col("city").desc_nulls_last())
)
