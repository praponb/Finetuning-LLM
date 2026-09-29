from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.upper("product_name").like("%OGA%"))
    .select("id", "supplier_id", "launch_date", F.ntile(4).over(w).alias("quartile"))
)
