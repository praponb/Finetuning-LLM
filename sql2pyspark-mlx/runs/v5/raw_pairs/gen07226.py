from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("units") >= 16)
    .select(
        "id",
        "sale_date",
        "revenue",
        (F.col("revenue") - F.lag("revenue", 1, 0).over(w)).alias("revenue_change"),
    )
)
