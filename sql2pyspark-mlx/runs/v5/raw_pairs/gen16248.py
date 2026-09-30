from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "product_id",
        "units",
        F.row_number().over(w).alias("sale_date_row"),
        F.last("sale_date").over(w2).alias("last_sale_date"),
    )
)
