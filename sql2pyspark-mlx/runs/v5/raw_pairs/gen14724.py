from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 6]))
    .select(
        "id",
        "product_id",
        "revenue",
        "store_id",
        "sale_date",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 3).alias("revenue_share"),
        F.rank().over(w2).alias("rnk"),
    )
)
