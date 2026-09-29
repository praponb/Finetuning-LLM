from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3, 4]))
    .select(
        "id",
        "units",
        "revenue",
        F.sum("units").over(w).alias("running_units"),
        F.row_number().over(w2).alias("seq_no"),
        F.last("units", ignorenulls=True).over(w3).alias("last_known_units"),
    )
)
