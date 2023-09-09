import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print('Usage : mmcounter <file>', file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("PythonMMCounter").getOrCreate()

    mmfile = sys.argv[1]

    mmdf = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(mmfile)

    countmmdf = mmdf \
        .select("State", "Color", "Count") \
        .groupby("State", "Color") \
        .agg(count("Count").alias("Total")) \
        .orderBy("Total", ascending=False)

    countmmdf.show(60, truncate=False)

    print(f'Total rows : {countmmdf.count()}')

    ca_countmmdf = mmdf \
        .select("State", "Color", "Count") \
        .where(mmdf.State == "CA") \
        .groupby("State", "Color") \
        .agg(count("Count").alias("Total")) \
        .orderBy("Total", ascending=False)

    ca_countmmdf.show(60, truncate=False)

    spark.stop()
