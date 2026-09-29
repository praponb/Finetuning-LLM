from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.max("revenue").alias("max_store_sale_revenue"))

metrics = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.ntile(3).over(w).alias("revenue_bucket"),
        F.avg("units").over(w2).alias("units_avg_6"),
        F.sum("units").over(w3).alias("running_units"),
    )
)

result = (
    metrics
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue_bucket",
        F.col("max_store_sale_revenue"),
    )
)
