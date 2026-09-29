from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("category")
w3 = Window.orderBy(F.col("stock_qty").desc(), "id")

joined = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.rating"))
)

kept = (
    joined
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.unit_price") < 150), "left_anti")
)

scored = (
    kept
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_3_prior"),
        F.corr("stock_qty", "price").over(w2).alias("corr_stock_qty_price"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.rating").alias("supplier_rating"))
)

result = (
    enriched
    .withColumn("rating_avg_3_prior_tier", F.when(F.col("rating_avg_3_prior") >= 4, "PREMIUM").otherwise("STANDARD"))
    .cube("seq_no", "rating_avg_3_prior_tier")
    .agg(
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("price")).otherwise(0)).alias("initech_price"),
        F.countDistinct("category").alias("distinct_categories"),
        F.percentile_approx("rating_avg_3_prior", 0.5).alias("rating_avg_3_prior_p50"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.grouping("seq_no").alias("gid"),
    )
    .orderBy(F.col("rating_avg_3_prior_tier").desc())
)
