from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2024-12-01")
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.round(F.avg("units").over(w), 2).alias("units_moving_avg_7"),
    )
)
