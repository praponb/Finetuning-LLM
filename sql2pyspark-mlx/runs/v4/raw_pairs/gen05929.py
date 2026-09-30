from pyspark.sql import functions as F

result = (
    df_products
    .rollup("category")
    .agg(
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.stddev_samp("cost").alias("cost_stddev"),
    )
    .orderBy(F.col("category").desc_nulls_last())
)
