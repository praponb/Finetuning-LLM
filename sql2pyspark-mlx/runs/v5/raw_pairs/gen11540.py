from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        "sale_date",
        "units",
        F.first("sale_date").over(w).alias("first_sale_date"),
        F.last("units").over(w2).alias("last_units"),
    )
    .orderBy("first_sale_date")
)
