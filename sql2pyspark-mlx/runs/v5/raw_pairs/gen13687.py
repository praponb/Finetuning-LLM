from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("is_singapore", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").asc_nulls_first())

filtered = (
    df_suppliers
    .filter(F.col("city") == "Singapore")
    .withColumn("is_singapore", F.when(F.col("city") == "Singapore", 1).otherwise(0))
)

result = (
    filtered
    .select(
        "id",
        "supplier_name",
        "city",
        F.first("is_singapore", ignorenulls=True).over(w).alias("first_known_is_singapore"),
        F.rank().over(w2).alias("rating_rank"),
    )
)
