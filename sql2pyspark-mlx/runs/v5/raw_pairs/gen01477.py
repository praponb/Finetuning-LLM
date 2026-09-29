from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.sum("units").over(w).alias("running_units"),
    )
)
