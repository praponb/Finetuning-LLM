from pyspark.sql import functions as F

unpivoted = (
    df_orders
    .select(
        "id",
        "store_id",
        F.col("amount").alias("AMOUNT"),
        "discount",
        F.col("shipping_fee").alias("SHIPPING_FEE_VAL"),
    )
    .unpivot(
        ["id", "store_id"],
        ["AMOUNT", "discount", "SHIPPING_FEE_VAL"],
        "measure",
        "metric_value",
    )
)

result = (
    unpivoted
    .groupBy(
        F.when(F.col("metric_value") >= 62, "LARGE").otherwise("SMALL").alias("metric_value_tier"),
    )
    .agg(
        F.max("metric_value").alias("max_metric_value"),
        F.sum(F.when(F.col("measure") == "discount", F.col("metric_value")).otherwise(0)).alias("discount_metric_value"),
        F.size(F.collect_set("measure")).alias("num_measures"),
    )
)
