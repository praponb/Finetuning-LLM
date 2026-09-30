from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_first())
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("rating").desc())
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_suppliers
    .select(
        "*",
        F.rank().over(w).alias("rating_rank"),
        F.sum("rating").over(w2).alias("cumulative_rating"),
        F.percent_rank().over(w3).alias("rating_percent_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w4)), 4).alias("rating_share"),
        F.last("city", ignorenulls=True).over(w5).alias("last_known_city"),
    )
)

result = (
    scored
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "cumulative_rating",
        "rating_share",
        "last_known_city",
        F.when(F.col("supplier_name").rlike("^(Siam Parts Co|EuroGoods GmbH)$"), "CORE").otherwise("PRIORITY").alias("supplier_name_zone"),
    )
)
