from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("cost", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id")
w_hist = Window.partitionBy("brand")
w_hist_2 = Window.partitionBy("category")
w6 = Window.partitionBy("supplier_id", "category")
w_ordered = Window.partitionBy("brand").orderBy("category_count_cost_bucket")

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("launch_date") >= F.lit("2023-04-27").cast("date")))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "supplier_id",
        F.round((F.col("stock_qty") / F.col("cost")), 2).alias("stock_qty_per_cost"),
        F.add_months("launch_date", 1).alias("launch_date_plus_months"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.ntile(4).over(w_grp).alias("cost_bucket"),
        F.first("brand", ignorenulls=True).over(w2).alias("first_known_brand"),
        F.avg("cost").over(w3).alias("supplier_id_avg_cost"),
    )
)

remaining_keys = (
    metrics
    .select("brand", "category")
    .exceptAll(
        df_products
        .filter(F.array_contains("tags", "new"))
        .select("brand", "category")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .withColumn("brand_count_cost_bucket", F.count("cost_bucket").over(w_hist))
    .withColumn("category_count_cost_bucket", F.count("cost_bucket").over(w_hist_2))
    .withColumn("corr_cost_stock_qty", F.corr("cost", "stock_qty").over(w_hist_2))
    .withColumn("category_sum_supplier_id_avg_cost", F.sum("supplier_id_avg_cost").over(w_hist_2))
    .withColumn("supplier_id_sum_stock_qty", F.sum("stock_qty").over(w3))
)

scored = (
    enriched
    .withColumn("supplier_id_avg_cost_share", F.round((F.col("supplier_id_avg_cost") / F.sum("supplier_id_avg_cost").over(w6)), 4))
    .withColumn("cost_bucket_share", F.round((F.col("cost_bucket") / F.sum("cost_bucket").over(w_hist)), 4))
    .withColumn("category_count_cost_bucket_percent_rank", F.percent_rank().over(w_ordered))
)

result = (
    scored
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .groupingSets(
        [["country", "category"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("e.category").alias("category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("e.category_count_cost_bucket")), 2).alias("avg_category_count_cost_bucket"),
        F.grouping_id().alias("grouping_level"),
    )
)
