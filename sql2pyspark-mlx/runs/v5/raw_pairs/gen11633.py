from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("starts_on", "id")

ranked = (
    df_promotions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "channel", "starts_on", "ends_on")
)

result = (
    ranked
    .groupBy(F.date_trunc("quarter", "ends_on").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.size(F.collect_set("channel")).alias("num_channels_2"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
    )
    .filter(F.col("web_rows") > 1)
    .orderBy(F.col("num_channels_2").desc_nulls_last())
)
