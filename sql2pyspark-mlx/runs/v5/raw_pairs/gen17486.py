from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.launch_date")), "left_semi")
)

result = (
    not_flagged
    .cube(F.date_trunc("WEEK", "launch_date").alias("week_start"), "supplier_id")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.stddev_samp("price").alias("price_stddev"),
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.corr("price", "cost").alias("corr_price_cost"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)
