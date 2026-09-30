from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_first())
w_ordered = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        F.lag("rating", 2, 0).over(w_grp).alias("prev_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
        F.rank().over(w3).alias("rating_rank"),
        F.avg("rating").over(w_ordered).alias("rating_avg_3"),
        F.ntile(3).over(w_grp).alias("rating_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "supplier_name",
        "prev_rating",
        "rating_share",
        "rating_avg_3",
        "rating_bucket",
    )
    .orderBy(F.col("prev_rating").desc())
)
