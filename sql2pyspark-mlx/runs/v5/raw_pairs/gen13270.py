from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(-6, -1)

metrics = (
    df_store_sales
    .filter(F.col("sale_date") <= "2023-03-01")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.first("units", ignorenulls=True).over(w).alias("first_known_units"),
        F.corr("revenue", "units").over(w2).alias("corr_revenue_units"),
        F.last("revenue", ignorenulls=True).over(w3).alias("last_known_revenue"),
        F.avg("revenue").over(w_running).alias("revenue_avg_6_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "product_id",
        "units",
        "first_known_units",
        "corr_revenue_units",
        "last_known_revenue",
        "revenue_avg_6_prior",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
