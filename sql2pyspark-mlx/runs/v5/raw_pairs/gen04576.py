from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc())

result = (
    df_products
    .select(
        "id",
        "supplier_id",
        "cost",
        "price",
        F.round((F.col("cost") / F.sum("cost").over(w)), 3).alias("cost_share"),
        F.rank().over(w2).alias("price_rank"),
    )
)
