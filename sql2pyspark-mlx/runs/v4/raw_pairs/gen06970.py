from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("sale_date_month_end", "id").rowsBetween(-5, -1)
w3 = Window.orderBy("sale_date_month_end", "id")
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_last())
w6 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units_percent_rank").desc_nulls_first(), "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

base = (
    df_store_sales
    .filter(F.col("store_id") == 4)
    .select("*", F.last_day("sale_date").alias("sale_date_month_end"))
)

with_windows = (
    base
    .select(
        "*",
        F.first("units", ignorenulls=True).over(w).alias("first_known_units"),
        F.avg("revenue").over(w2).alias("revenue_avg_5_prior"),
        F.lead("revenue", 1).over(w3).alias("next_revenue"),
    )
)

with_windows_store_sales = (
    with_windows
    .select(
        "*",
        F.lag("revenue_avg_5_prior", 1).over(w3).alias("prev_revenue_avg_5_prior"),
        F.round((F.col("units") / F.sum("units").over(w4)), 4).alias("units_share"),
        F.percent_rank().over(w5).alias("units_percent_rank"),
    )
)

ranked = (
    with_windows_store_sales
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue_avg_5_prior",
        "next_revenue",
        "prev_revenue_avg_5_prior",
        F.col("avg_inventory_quantity"),
    )
)
