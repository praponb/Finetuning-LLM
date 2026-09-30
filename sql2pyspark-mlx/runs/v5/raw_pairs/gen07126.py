from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .filter(F.col("price") > 370)
    .select("id", "cost", F.row_number().over(w).alias("row_num"))
)
