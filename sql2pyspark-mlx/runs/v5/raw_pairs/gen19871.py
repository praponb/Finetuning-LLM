from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("supplier_id", "category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy(F.col("running_avg_cost").desc_nulls_last(), "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("cost").alias("avg_product_cost"))

latest = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    latest
    .select(
        "*",
        F.avg("cost").over(w2).alias("running_avg_cost"),
        F.count("cost").over(w2).alias("running_count_cost"),
        F.min("cost").over(w2).alias("running_min_cost"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.city"), F.col("p.country"), F.col("p.rating"))
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.month(F.col("x.sale_date")).isin([6, 7, 9])), "left_semi")
)

summary = (
    kept
    .groupBy(
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
        "supplier_id",
    )
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_count"),
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("running_count_cost")).otherwise(0)).alias("umbrella_running_count_cost"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("price")).otherwise(0)).alias("electronics_price"),
    )
    .filter(F.col("umbrella_running_count_cost") > 0)
)

result = (
    summary
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "quarter_start",
        "supplier_id",
        "category_list",
        "acme_count",
        F.col("avg_product_cost"),
    )
)
