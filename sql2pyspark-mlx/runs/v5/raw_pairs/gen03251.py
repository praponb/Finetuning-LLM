from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        F.corr("units", "revenue").over(w).alias("corr_units_revenue"),
        F.last("units", ignorenulls=True).over(w2).alias("last_known_units"),
        F.rank().over(w3).alias("units_rank"),
    )
)
