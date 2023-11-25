import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# spark-submit --packages com.databricks:spark-xml_2.12:0.15.0 --master local PySpark.py

infobox_regex = r"\n\|(.*?)=(.*)"
dump_file = "E:\\VINF_data\\enwiki-20230820-pages-articles-multistream.xml"

def load_gazetteer(path="src_code/gazetteer.txt") -> list[str]:
	entities = []
	with open(path, "r", encoding='utf-8') as txt_file:
		for line in txt_file:
			entities.append(line.strip())
	return entities

def map_many(iterable, function, *other):
    if other:
        return map_many(map(function, iterable), *other)
    return map(function, iterable)

def clean_text(data: list[str]) -> list[str]:
	new_data = []
	for item in data:
		new_text = item.strip()
		new_text = re.sub(r"[\[\]\{\}\=]|<.*?>", "", new_text)
		new_text = re.sub(r"\|", " ", new_text)
		new_text = re.sub(r" +", " ", new_text)
		new_data.append(new_text)
	return new_data

def create_session() -> SparkSession:
	spark_sess = SparkSession.builder.appName("regex_example").getOrCreate()
	return spark_sess

# Inicializácia Spark Session
gazetteer = load_gazetteer()
spark = create_session()
clean_data = udf(clean_text, ArrayType(StringType()))

# Načítanie textového súboru
df = spark.read.format("com.databricks.spark.xml").options(rootTag="mediawiki", rowTag="page", encoding="ISO-8859-1").load(dump_file)
df = df.filter(col("title").isin(gazetteer))

# Extrahovanie dátumu z každého riadku
extracted_df = df.withColumns({"reg_key": regexp_extract_all('revision.text._VALUE', lit(infobox_regex), 1),
							   "reg_value": regexp_extract_all('revision.text._VALUE', lit(infobox_regex), 2)})

# Zobrazenie výsledkov
ne_df = extracted_df.filter(size(col("reg_key")) > 0)
mod_df = ne_df.withColumns({"mod_key": clean_data(ne_df.reg_key),
							"mod_val": clean_data(ne_df.reg_value)})
# all_keys = [{row["title"]: dict(zip(list(map(str.strip, row["reg_key"])), list(map_many(row["reg_value"], str.strip, clean_text))))} for row in row_keys]
final_data = mod_df.select("title", "mod_key", "mod_val")

final_data.write.mode("overwrite").json("spark_output")

# Ukončenie Spark Session
spark.stop()
