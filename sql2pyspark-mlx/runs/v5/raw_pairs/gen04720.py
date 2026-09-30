from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_suppliers
    .select(
        "*",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.first("city", ignorenulls=True).over(w2).alias("first_known_city"),
    )
)

result = (
    scored
    .select("id", "supplier_name", "city", "first_known_city")
    .orderBy(F.col("city").desc())
)
