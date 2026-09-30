from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id")

with_windows = (
    df_products
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.first("cost", ignorenulls=True).over(w).alias("first_known_cost"),
        F.sum("price").over(w2).alias("cumulative_price"),
        F.corr("cost", "price").over(w3).alias("corr_cost_price"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("QUARTER", "launch_date").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.size(F.collect_set("category")).alias("num_categories_2"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_rows"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.sort_array(F.collect_set("brand")).alias("brand_list_2"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
