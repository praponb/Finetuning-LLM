from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id")
w2 = Window.partitionBy("product_id")

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-02-18").cast("date")) & F.col("units").isNotNull())
    .select("*", (F.col("revenue") * F.col("units")).alias("revenue_per_unit"))
)

result = (
    base
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.lead("revenue", 2).over(w).alias("next_revenue"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
    )
)
