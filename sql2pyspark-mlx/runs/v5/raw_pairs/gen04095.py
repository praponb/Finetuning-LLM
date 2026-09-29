from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())
w2 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .select(
        "id",
        "product_id",
        "revenue",
        "sale_date",
        "units",
        F.rank().over(w).alias("rnk"),
        F.sum("units").over(w2).alias("running_units"),
    )
)
