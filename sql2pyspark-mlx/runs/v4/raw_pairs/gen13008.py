from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w_ordered = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .select(
        "*",
        F.count("rating").over(w).alias("country_count_rating"),
        F.sum("rating").over(w_ordered).alias("cumulative_rating"),
    )
)
