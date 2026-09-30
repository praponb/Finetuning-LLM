from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1600)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.lead("units", 1).over(w).alias("next_units"),
        F.rank().over(w2).alias("rnk"),
    )
)
