from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy("cost", "id")

base = (
    df_products
    .filter((~F.col("brand").eqNullSafe(F.lit("Initech"))) & (F.col("launch_date") > F.lit("2023-09-17").cast("date")))
    .select("*", F.upper("brand").alias("brand_upper"))
)

result = (
    base
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "brand_upper",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.first("price", ignorenulls=True).over(w2).alias("first_known_price"),
        F.lag("stock_qty", 1).over(w3).alias("prev_stock_qty"),
    )
)
