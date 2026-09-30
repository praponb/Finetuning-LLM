from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w3 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w5 = Window.orderBy(F.col("units").asc_nulls_last())

scored = (
    df_store_sales
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("seq_no_2", F.row_number().over(w2))
    .withColumn("cumulative_units", F.sum("units").over(w3))
    .withColumn("next_units", F.lead("units", 2, 0).over(w4))
    .withColumn("units_rank", F.rank().over(w5))
)

result = (
    scored
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "seq_no",
        "seq_no_2",
        "cumulative_units",
    )
    .orderBy(F.col("seq_no_2").desc(), "id")
)
