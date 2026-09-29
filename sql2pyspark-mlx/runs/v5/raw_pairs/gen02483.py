from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id", "store_id").orderBy("revenue_per_unit_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")
w3 = Window.partitionBy("product_id", "store_id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("revenue_per_unit_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-19").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit_2"),
    )
)

result = (
    valid
    .filter(F.col("units") >= 19)
    .select(
        "*",
        F.sum("revenue_per_unit").over(w_recent).alias("running_revenue_per_unit"),
        F.lead("units", 2, 0).over(w_recent_2).alias("next_units"),
        F.round((F.col("units") / F.sum("units").over(w3)), 4).alias("units_share"),
        F.sum("units").over(w4).alias("running_units"),
    )
)
