from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("units") != 28)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        (F.col("units") - F.lag("units", 1).over(w)).alias("units_change"),
    )
)
