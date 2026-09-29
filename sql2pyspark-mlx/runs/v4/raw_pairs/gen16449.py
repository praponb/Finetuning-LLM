from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
w2 = Window.partitionBy("supplier_id").orderBy("price", "id")
w3 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category")
w5 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("brand").orderBy(F.col("prev_days_since_launch_date").desc(), "id")
w7 = Window.partitionBy("supplier_id", "category").orderBy("cost", "id")
w8 = Window.partitionBy("category", "brand")
w9 = Window.partitionBy("category").orderBy(F.col("cost").asc_nulls_first(), "id")
w_recent = Window.orderBy("prev_first_known_least_cost_stock_qty_minus_price", "id").rowsBetween(-5, -1)
w_grp = Window.partitionBy("supplier_id").orderBy(F.col("first_known_least_cost_stock_qty").desc(), "id")
w12 = Window.partitionBy("supplier_id").orderBy("category_avg_first_known_least_cost_stock_qty", "id").rowsBetween(-5, Window.currentRow)
w13 = Window.partitionBy("supplier_id")

valid = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-11-28").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.least("cost", "stock_qty").alias("least_cost_stock_qty"),
    )
)

enriched = (
    valid
    .withColumn("prev_days_since_launch_date", F.lag("days_since_launch_date", 2).over(w))
    .withColumn("price_bucket", F.ntile(10).over(w2))
    .withColumn("first_known_least_cost_stock_qty", F.first("least_cost_stock_qty", ignorenulls=True).over(w3))
    .withColumn("category_avg_cost", F.avg("cost").over(w4))
    .withColumn("cumulative_days_since_launch_date", F.sum("days_since_launch_date").over(w5))
)

base = (
    enriched
    .filter(F.col("launch_date") > F.lit("2023-06-06").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "days_since_launch_date",
        "least_cost_stock_qty",
        "prev_days_since_launch_date",
        "price_bucket",
        "first_known_least_cost_stock_qty",
        "category_avg_cost",
        "cumulative_days_since_launch_date",
        (F.col("first_known_least_cost_stock_qty") - F.col("price")).alias("first_known_least_cost_stock_qty_minus_price"),
        F.round((F.col("first_known_least_cost_stock_qty") * 1.07), 2).alias("adjusted_first_known_least_cost_stock_qty"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

scored = (
    base
    .withColumn("prev_days_since_launch_date_bucket", F.ntile(5).over(w6))
    .withColumn("prev_first_known_least_cost_stock_qty_minus_price", F.lag("first_known_least_cost_stock_qty_minus_price", 2).over(w7))
    .withColumn("category_avg_first_known_least_cost_stock_qty", F.avg("first_known_least_cost_stock_qty").over(w8))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w9))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .withColumn("running_stock_qty", F.sum("stock_qty").over(w5))
    .withColumn("days_since_launch_date_avg_5_prior", F.avg("days_since_launch_date").over(w_recent))
    .withColumn("first_known_least_cost_stock_qty_bucket", F.ntile(10).over(w_grp))
    .withColumn("first_known_least_cost_stock_qty_avg_5", F.avg("first_known_least_cost_stock_qty").over(w12))
    .withColumn("corr_stock_qty_cost", F.corr("stock_qty", "cost").over(w13))
)

result = (
    with_windows
    .groupBy(F.date_trunc("WEEK", "launch_date").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("category") == "Electronics", F.col("first_known_least_cost_stock_qty_minus_price")).otherwise(0)).alias("electronics_first_known_least_cost_stock_qty_minus_price"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
    )
)
