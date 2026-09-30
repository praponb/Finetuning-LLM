from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("delivered_date").desc(), "id")
w2 = Window.partitionBy("carrier")
w_grp = Window.partitionBy("status", "order_id")
w4 = Window.partitionBy("order_id").orderBy("cost", "id")
w5 = Window.orderBy("delivered_date", "id")

scored = (
    df_shipments
    .filter(F.col("cost").between(33, 72))
    .select(
        "id",
        "carrier",
        "status",
        F.row_number().over(w).alias("seq_no"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.count("cost").over(w_grp).alias("status_count_cost"),
        F.ntile(3).over(w4).alias("cost_bucket"),
        F.lead("cost", 1).over(w5).alias("next_cost"),
    )
)

result = (
    scored
    .select(
        "id",
        "carrier",
        "cost_share",
        "status_count_cost",
        "cost_bucket",
        F.when(F.col("carrier").rlike("^(UPS|Flash)$"), "PRIORITY").when(F.col("carrier").rlike("^(DHL|FedEx)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("carrier_zone"),
    )
)
