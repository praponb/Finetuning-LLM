from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").asc_nulls_last(), "id")

deduped = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .withColumn("discount_tier", F.when(F.col("discount") >= 0.41, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["status", "discount_tier"], ["status"], []],
        "status",
        "discount_tier",
    )
    .agg(
        F.size(F.collect_set("channel")).alias("num_channels"),
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
        F.grouping("status").alias("gid"),
    )
    .filter(F.col("num_channels") > 1)
)
