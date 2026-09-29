from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Books", "Grocery"]))
    .groupBy("supplier_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.avg("cost").alias("avg_cost"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .orderBy(F.col("avg_cost").desc(), "supplier_id")
    .limit(1)
)
