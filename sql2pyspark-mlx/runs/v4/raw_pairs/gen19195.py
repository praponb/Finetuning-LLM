from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-04-29"))) & F.col("supplier_id").isNotNull() & (F.col("price") <= 760))
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.col("price").cast("int").alias("price_int"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

result = (
    filtered
    .withColumn("quarter_start", F.date_trunc("quarter", "launch_date"))
    .groupingSets(
        [["quarter_start", "supplier_id"], ["quarter_start"]],
        "quarter_start",
        "supplier_id",
    )
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.percentile_approx("price_int", [0.25, 0.5, 0.75]).alias("price_int_quartiles"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("supplier_id")
)
