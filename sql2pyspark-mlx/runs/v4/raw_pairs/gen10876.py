from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("stock_qty") >= 145))
    .withColumn("price_band", F.when(F.col("price") >= 950, "High").when(F.col("price") >= 530, "Medium").otherwise("Low"))
    .withColumn("category_upper", F.upper("category"))
)

result = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
