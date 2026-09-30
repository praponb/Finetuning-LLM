from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w_recent = Window.partitionBy("product_id").orderBy("revenue", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("stock_qty").desc())
w5 = Window.partitionBy("product_id").orderBy("sale_date", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-11-15").cast("date"))
    .select(
        "*",
        F.ceil("revenue").alias("revenue_whole"),
        F.ceil("revenue").alias("revenue_whole_2"),
    )
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.brand"), F.col("d.stock_qty"), F.col("d.price"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.avg("revenue_whole").over(w).alias("revenue_whole_avg_5"),
        F.lag("revenue", 2).over(w2).alias("prev_revenue"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.percent_rank().over(w4).alias("stock_qty_percent_rank"),
        F.lag("revenue_whole", 2).over(w5).alias("prev_revenue_whole"),
    )
)

valid = (
    enriched
    .filter(F.col("event_seq") >= 4)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue_whole",
        "revenue_whole_2",
        "brand",
        "price",
        "revenue_whole_avg_5",
        "prev_revenue",
        "event_seq",
        "stock_qty_percent_rank",
        "prev_revenue_whole",
        F.when(F.col("brand").isin(["Stark", "Initech"]), "Group A").otherwise("Other").alias("brand_group"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

result = (
    valid
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue_whole",
        "price",
        "revenue_whole_avg_5",
        "stock_qty_percent_rank",
        "prev_revenue_whole",
    )
)
