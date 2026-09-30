from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id", "store_id")

metrics = (
    df_store_sales
    .select(
        "*",
        F.avg("units").over(w).alias("store_id_avg_units"),
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
    )
)

result = (
    metrics
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "store_id_avg_units",
        "units_share",
    )
    .orderBy(F.col("store_id_avg_units").desc_nulls_first(), "units_share")
)
