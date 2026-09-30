from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("least_revenue_units", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-12-16")))
    .select(
        "*",
        F.least("revenue", "units").alias("least_revenue_units"),
        F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"),
    )
)

result = (
    cleaned
    .select(
        "*",
        F.corr("units", "revenue").over(w_part).alias("corr_units_revenue"),
        F.first("least_revenue_units", ignorenulls=True).over(w2).alias("first_known_least_revenue_units"),
        F.lead("revenue", 1).over(w3).alias("next_revenue"),
    )
)
