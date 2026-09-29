from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")
w_part = Window.partitionBy("store_id", "product_id")
w3 = Window.partitionBy("product_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.lead("revenue", 1).over(w).alias("next_revenue"),
        F.corr("revenue", "units").over(w_part).alias("corr_revenue_units"),
        F.round((F.col("revenue") / F.sum("revenue").over(w_part)), 4).alias("revenue_share"),
        F.lag("units", 1, 0).over(w3).alias("prev_units"),
    )
)
