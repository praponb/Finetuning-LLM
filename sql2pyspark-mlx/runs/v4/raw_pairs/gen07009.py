from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

with_windows = (
    df_stores
    .withColumn("last_known_sqft", F.last("sqft", ignorenulls=True).over(w_ordered))
    .withColumn("sqft_avg_5", F.avg("sqft").over(w_part))
    .withColumn("seq_no", F.row_number().over(w3))
)

result = (
    with_windows
    .select(
        "id",
        "city",
        "opened_date",
        "last_known_sqft",
        "sqft_avg_5",
        "seq_no",
        F.when(F.col("city").rlike("^(Bangkok|Tokyo)$"), "GROUP_B").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "CORE").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("opened_date").desc_nulls_last())
)
