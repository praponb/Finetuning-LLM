from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("category").orderBy(F.col("launch_date").desc())

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("supplier_id").isin([1, 10, 11, 18]))
    .withColumn("product_name_category_key", F.concat_ws("-", "product_name", "category"))
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

latest = (
    ranked
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

pivoted = (
    latest
    .groupBy("supplier_id")
    .pivot("brand", ["Initech", "Umbrella", "Globex"])
    .agg(F.max("price"))
)

result = (
    pivoted
    .select("supplier_id", "Initech", "Globex")
    .orderBy(F.col("supplier_id").desc(), "Globex")
)
