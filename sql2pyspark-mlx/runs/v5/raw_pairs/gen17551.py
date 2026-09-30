from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_last())
w_hist = Window.partitionBy("product_id").orderBy("revenue", "id")
w3 = Window.partitionBy("product_id")
w_ordered = Window.partitionBy("store_id")
w_running = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-04-01").cast("date")) & F.col("units").between(29, 40))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM-dd"))
)

result = (
    base
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "sale_date_label",
        F.rank().over(w_recent).alias("revenue_rank"),
        F.lead("units", 1).over(w_hist).alias("next_units"),
        F.round((F.col("units") / F.sum("units").over(w3)), 4).alias("units_share"),
        F.corr("revenue", "units").over(w_ordered).alias("corr_revenue_units"),
        F.first("revenue", ignorenulls=True).over(w_running).alias("first_known_revenue"),
    )
)
