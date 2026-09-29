from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .select("id", "sale_date", "units", F.lead("units", 2).over(w).alias("next_units"))
)
