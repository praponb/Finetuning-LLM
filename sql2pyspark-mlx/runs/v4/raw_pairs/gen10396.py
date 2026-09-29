from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
)
