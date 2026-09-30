from pyspark.sql import functions as F

kept = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left_semi")
)

result = (
    kept
    .withColumn("quarter_start", F.date_trunc("quarter", "launch_date"))
    .groupingSets(
        [["quarter_start", "brand"], ["quarter_start"], []],
        "quarter_start",
        "brand",
    )
    .agg(
        F.countDistinct("category").alias("distinct_categories"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.count_if(F.col("brand") == "Stark").alias("stark_events"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("category") == "Electronics", 1)), F.lit(0)), 2).alias("cost_per_electronics"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("cost")).otherwise(0)).alias("initech_cost"),
        F.grouping("quarter_start").alias("gid"),
    )
)
