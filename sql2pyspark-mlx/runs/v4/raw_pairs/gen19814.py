from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("shipped_date_minus_20d").desc_nulls_last(), "id")

valid = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-03-10").cast("date")) & F.col("order_id").isNotNull())
    .select("*", F.date_sub("shipped_date", 20).alias("shipped_date_minus_20d"))
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    latest
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.discount"))
)

stats = (
    joined
    .withColumn("year_start", F.date_trunc("YEAR", "shipped_date_minus_20d"))
    .cube("year_start", "order_id")
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.min("cost").alias("min_cost"),
        F.size(F.collect_set("carrier")).alias("num_carriers_2"),
        F.grouping("year_start").alias("grouping_level"),
    )
    .filter(F.col("num_carriers_2") >= 1)
)

result = stats.select("year_start", "order_id", "min_cost", "grouping_level")
