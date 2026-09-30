from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("stock_qty_filled", "id")
w2 = Window.partitionBy("product_id")
w_part = Window.partitionBy("product_id").orderBy(F.col("stock_qty_filled").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-07-15").cast("date"))
    .select("id", "store_id", "product_id", F.ceil("units").alias("units_whole"))
)

joined = (
    valid
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.cost").between(400, 460))
    .select("src.id", "src.product_id", F.col("b.stock_qty"))
)

filtered = (
    joined
    .filter(F.col("stock_qty") < 187)
    .select("*", F.coalesce("stock_qty", F.lit(0)).alias("stock_qty_filled"))
)

scored = (
    filtered
    .select(
        "id",
        "product_id",
        F.lead("stock_qty", 1).over(w).alias("next_stock_qty"),
        F.count("stock_qty").over(w2).alias("product_id_count_stock_qty"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

result = (
    scored
    .alias("a")
    .join(df_products.alias("dim"), F.col("a.product_id") == F.col("dim.id"), "inner")
    .rollup(F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.next_stock_qty")).alias("min_next_stock_qty"),
        F.min(F.col("a.product_id_count_stock_qty")).alias("min_product_id_count_stock_qty"),
    )
)
