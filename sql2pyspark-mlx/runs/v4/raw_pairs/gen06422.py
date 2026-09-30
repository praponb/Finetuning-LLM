from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")
w_hist = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_suppliers
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.last("rating", ignorenulls=True).over(w_hist).alias("last_known_rating"),
    )
)

result = (
    metrics
    .select(
        "id",
        "country",
        "city",
        "rating",
        "row_seq",
        "last_known_rating",
        F.when(F.col("city").rlike("^(London|Berlin)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").otherwise("CORE").alias("city_zone"),
    )
)
