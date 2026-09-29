from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.year("sale_date") >= 2023)
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        F.lead("revenue", 2).over(w).alias("next_revenue"),
    )
)
