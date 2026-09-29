from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id", "product_id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "store_id",
        F.lead("units", 1).over(w).alias("next_units"),
        F.round((F.col("units") / F.sum("units").over(w2)), 3).alias("units_share"),
    )
)
