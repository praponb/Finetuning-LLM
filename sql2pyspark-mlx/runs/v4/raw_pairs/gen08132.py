from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id")
w2 = Window.orderBy("greatest_units_revenue", "id")

filtered = (
    df_store_sales
    .filter(F.col("revenue").between(2300, 2800))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        F.add_months("sale_date", 12).alias("sale_date_plus_months"),
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
    )
)

result = (
    filtered
    .withColumn("next_greatest_units_revenue", F.lead("greatest_units_revenue", 1).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)
