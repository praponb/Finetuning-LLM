from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-08-24").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        F.dayofmonth("launch_date").alias("launch_date_day"),
    )
)

result = (
    filtered
    .withColumn("stock_qty_tier", F.when(F.col("stock_qty") >= 405, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["stock_qty_tier", "category"], ["stock_qty_tier"]],
        "stock_qty_tier",
        "category",
    )
    .agg(
        F.sum(F.when(F.col("category") == "Home", F.col("cost"))).alias("home_total_cost"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.corr("stock_qty", "cost").alias("corr_stock_qty_cost"),
        F.avg(F.when(F.col("category") == "Fashion", F.col("stock_qty"))).alias("fashion_avg_stock_qty"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
    )
    .orderBy(F.col("corr_stock_qty_cost").asc_nulls_last())
)
