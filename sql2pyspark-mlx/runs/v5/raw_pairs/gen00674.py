from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("supplier_id").orderBy("stock_qty", "id")
w2 = Window.partitionBy("brand")

with_windows = (
    df_products
    .select(
        "*",
        F.lead("stock_qty").over(w_recent).alias("next_stock_qty"),
        F.lag("stock_qty").over(w_recent).alias("prev_stock_qty"),
        F.lag("cost").over(w_recent).alias("prev_cost"),
        F.lead("cost").over(w_recent).alias("next_cost"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.avg("cost").over(w2).alias("brand_avg_cost"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "launch_date",
        "prev_stock_qty",
        "prev_cost",
        "brand_avg_cost",
        F.when(F.col("category").rlike("^(Home|Books)$"), "GROUP_A").when(F.col("category").rlike("^(Electronics|Sports)$"), "CORE").otherwise("OTHER_ZONE").alias("category_zone"),
    )
    .orderBy(F.col("prev_stock_qty").desc_nulls_last())
)
