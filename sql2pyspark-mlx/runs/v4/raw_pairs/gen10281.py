from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("store_id") == 1)
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        (F.col("revenue") - F.lag("revenue", 2).over(w)).alias("revenue_change"),
    )
    .orderBy(F.col("product_id").desc())
)
