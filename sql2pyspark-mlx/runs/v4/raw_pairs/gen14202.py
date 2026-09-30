from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_suppliers
    .filter(F.col("country") == "DE")
    .select(
        "id",
        "city",
        "rating",
        "supplier_name",
        F.last("supplier_name").over(w).alias("last_supplier_name"),
    )
)
