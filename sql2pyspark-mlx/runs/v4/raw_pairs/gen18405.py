from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_products
    .select(
        "id",
        "brand",
        "launch_date",
        "cost",
        "supplier_id",
        "category",
        F.lead("cost", 1, 0).over(w).alias("next_cost"),
        F.last("category").over(w2).alias("last_category"),
    )
)
