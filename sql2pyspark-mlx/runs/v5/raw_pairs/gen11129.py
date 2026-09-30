from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("rating", "id")
w2 = Window.partitionBy("country", "city")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

scored = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Bangkok", "London"]))
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.lag("rating", 1, 0).over(w_grp).alias("prev_rating"),
        F.avg("rating").over(w2).alias("country_avg_rating"),
        F.sum("rating").over(w3).alias("running_rating"),
        F.avg("rating").over(w4).alias("rating_avg_5_prior"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "prev_rating",
        "rating_avg_5_prior",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
