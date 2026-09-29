from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.orderBy("revenue", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "sale_date",
        F.avg("units").over(w).alias("store_id_avg_units"),
        F.first("sale_date").over(w2).alias("first_sale_date"),
    )
)
