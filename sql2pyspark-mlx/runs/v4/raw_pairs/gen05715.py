from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 9]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "store_id",
        "units",
        F.first("revenue").over(w).alias("first_revenue"),
        F.round((F.col("units") / F.sum("units").over(w2)), 3).alias("units_share"),
    )
)
