from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2024-04-01")
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        F.lead("units", 1).over(w).alias("next_units"),
    )
)
