from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id")
w2 = Window.orderBy("launch_date", "id").rowsBetween(-3, -1)
w_hist = Window.orderBy("cost", "id")
w_part = Window.partitionBy("category").orderBy("price", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())

eligible = (
    df_products
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id") == 7), "left_anti")
)

metrics = (
    eligible
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        F.lag("cost").over(w).alias("prev_cost"),
        F.lag("price").over(w).alias("prev_price"),
        F.row_number().over(w).alias("row_seq"),
        F.avg("cost").over(w2).alias("cost_avg_3_prior"),
    )
)

scored = (
    metrics
    .withColumn("cost_bucket", F.ntile(3).over(w_hist))
    .withColumn("prev_cost_avg_2", F.avg("prev_cost").over(w_part))
)

latest_match = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.revenue").alias("store_sale_revenue"))
)

stats = (
    latest_match
    .withColumn("cost_avg_3_prior_tier", F.when(F.col("cost_avg_3_prior") >= 701, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["cost_avg_3_prior_tier", "category"], ["cost_avg_3_prior_tier"], []],
        "cost_avg_3_prior_tier",
        "category",
    )
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.grouping("cost_avg_3_prior_tier").alias("grouping_level"),
    )
)

result = (
    stats
    .select("cost_avg_3_prior_tier", "category", "total_stock_qty", "category_list")
)
