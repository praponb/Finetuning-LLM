from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("brand", "category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand").orderBy("first_known_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand")
w6 = Window.partitionBy("supplier_id").orderBy("price", "id")
w7 = Window.partitionBy("supplier_id")

metrics = (
    df_products
    .filter(F.col("supplier_id").isin([14, 30, 40]))
    .select(
        "*",
        F.sum("cost").over(w_grp).alias("running_sum_cost"),
        F.max("cost").over(w_grp).alias("running_max_cost"),
        F.avg("cost").over(w_grp).alias("running_avg_cost"),
        F.count("cost").over(w_grp).alias("running_count_cost"),
        F.first("cost", ignorenulls=True).over(w_ordered).alias("first_known_cost"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.launch_date")), "left_anti")
)

filtered = (
    not_flagged
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "*",
        (F.col("running_sum_cost") + F.col("running_max_cost")).alias("running_sum_cost_plus_running_max_cost"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

metrics_products = (
    filtered
    .select(
        "*",
        F.sum("price").over(w3).alias("cumulative_price"),
        F.sum("running_max_cost").over(w4).alias("cumulative_running_max_cost"),
        F.count("running_sum_cost").over(w5).alias("brand_count_running_sum_cost"),
        F.row_number().over(w6).alias("event_seq"),
        F.corr("stock_qty", "cost").over(w7).alias("corr_stock_qty_cost"),
    )
)

result = (
    metrics_products
    .cube(F.date_trunc("MONTH", "launch_date").alias("activity_month"), "supplier_id")
    .agg(
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_rows"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("category") == "Sports", F.col("running_max_cost"))).alias("sports_total_running_max_cost"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_count"),
        F.avg(F.when(F.col("category") == "Home", F.col("running_avg_cost"))).alias("home_avg_running_avg_cost"),
    )
)
