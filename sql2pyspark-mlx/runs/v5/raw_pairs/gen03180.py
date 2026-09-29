from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id")

result = (
    df_promotions
    .select(
        "id",
        "discount_pct",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.ntile(4).over(w2).alias("discount_pct_bucket"),
    )
)
