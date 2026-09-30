from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("units") >= 20)
    .select(
        "id",
        "sale_date",
        "revenue",
        F.lead("revenue", 2).over(w).alias("next_revenue"),
    )
)
