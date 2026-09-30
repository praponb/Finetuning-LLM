from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("category").orderBy("cost", "id")
w_part_2 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")
w5 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("category").orderBy("cumulative_cost", "id")
w7 = Window.partitionBy("brand", "category").orderBy(F.col("cost").desc())

scored = (
    df_products
    .select(
        "*",
        F.row_number().over(w_part).alias("event_seq"),
        F.avg("cost").over(w2).alias("cost_avg_6_prior"),
        F.ntile(10).over(w3).alias("cost_bucket"),
        F.row_number().over(w_part_2).alias("row_seq"),
        F.sum("cost").over(w5).alias("cumulative_cost"),
    )
)

valid = (
    scored
    .filter(F.col("supplier_id").isNotNull() & F.col("product_name").like("Ba%"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "cost_bucket",
        "row_seq",
        "cumulative_cost",
        F.round("price", 0).alias("price_rounded"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

scored_products = (
    valid
    .select(
        "*",
        F.row_number().over(w_running).alias("row_seq_2"),
        F.ntile(3).over(w_running).alias("cumulative_cost_bucket"),
        F.rank().over(w7).alias("cost_rank"),
    )
)

result = (
    scored_products
    .groupBy("brand")
    .pivot("category", ["Electronics", "Grocery"])
    .agg(F.avg("cost").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
