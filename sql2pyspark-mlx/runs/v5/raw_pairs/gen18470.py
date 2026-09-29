from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").desc())
w2 = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .filter(F.col("units") == 34)
    .select(
        "id",
        "units",
        "product_id",
        "sale_date",
        F.rank().over(w).alias("units_rank"),
        F.rank().over(w2).alias("rnk"),
    )
)
