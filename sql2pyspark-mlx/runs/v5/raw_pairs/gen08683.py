from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("units").alias("sum_store_sale_units"))

scored = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.sum("revenue").over(w_recent).alias("running_sum_revenue"),
        F.min("revenue").over(w_recent).alias("running_min_revenue"),
        F.max("revenue").over(w_recent).alias("running_max_revenue"),
        F.avg("revenue").over(w_recent).alias("running_avg_revenue"),
    )
)

result = (
    scored
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "running_sum_revenue",
        "running_avg_revenue",
        F.col("sum_store_sale_units"),
    )
    .orderBy(F.col("sum_store_sale_units").desc())
)
