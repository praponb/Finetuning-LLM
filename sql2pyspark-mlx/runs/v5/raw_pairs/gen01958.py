from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2023-08-01")
    .select(
        "id",
        "store_id",
        "units",
        "sale_date",
        F.first("sale_date").over(w).alias("first_sale_date"),
    )
)
