from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id").orderBy("units", "id")
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("brand").orderBy(F.col("units").desc())
w5 = Window.orderBy("prev_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.brand"))
)

with_windows = (
    joined
    .select(
        "*",
        F.lag("units", 2).over(w).alias("prev_units"),
        F.ntile(4).over(w2).alias("units_bucket"),
        F.corr("revenue", "units").over(w3).alias("corr_revenue_units"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w4).alias("units_dense_rank"),
        F.sum("revenue").over(w5).alias("cumulative_revenue"),
    )
)

result = (
    enriched
    .select(
        "id",
        "sale_date",
        "brand",
        "prev_units",
        "units_bucket",
        "corr_revenue_units",
        "cumulative_revenue",
        F.when(F.col("brand").rlike("^(Stark|Initech)$"), "GROUP_B").when(F.col("brand").rlike("^(Acme|Globex)$"), "CORE").otherwise("GROUP_A").alias("brand_zone"),
    )
)
