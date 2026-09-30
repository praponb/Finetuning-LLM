from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc())
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        "store_id",
        "revenue",
        F.percent_rank().over(w).alias("units_pct_rank"),
        F.first("revenue").over(w2).alias("first_revenue"),
    )
    .orderBy(F.col("first_revenue").desc(), "id")
)
