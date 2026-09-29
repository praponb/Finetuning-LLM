from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        F.sum("revenue").over(w).alias("running_revenue"),
    )
)
