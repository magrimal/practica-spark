import json
import pprint
import sys
from gensim.utils import tokenize

from pyspark.sql import SparkSession
import csv

def main(filename):
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    lines = sc.textFile(filename)
    words = lines.flatMap(lambda x: x.split())
    pairs = words.map(lambda x: (x,1))
    counts = pairs.reduceByKey(lambda x,y: x+y)
    print(counts.collect())
    

#if __name__ == "__main__":
    #main(sys.argv[1])
#    main("C:\\Users\\natal\\Documents\\ADatos\\practica3\\space.json")

def scispace(filename):
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    lines = sc.textFile(filename)
    json_objs = lines.map(lambda x: json.loads(x))
    filename_content = json_objs.map(lambda obj: (obj["filename"], obj["content"]))
    tokens_by_file = filename_content.flatMap(
        lambda x: [(x[0], t) for t in tokenize(x[1], to_lower=True)]
    )
    pairs_by_file = tokens_by_file.map(lambda x: ((x[0], x[1]), 1))
    counts_by_file = pairs_by_file.reduceByKey(lambda a, b: a + b)
    counts_by_file = counts_by_file.map(lambda x: (x[0][1], (x[0][0], x[1])))
    pprint.pprint(counts_by_file.collect())

def felicidad(filename):
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile(filename)

    def parse_csv(line):
        try:
            return next(csv.reader([line]))
        except:
            return []
        
    parsed = rdd.map(parse_csv)

    parsed = parsed.filter(lambda row: len(row) > 11)

    col12_rdd = parsed.map(lambda row: (row[0], row[11]))

    #print(col12_rdd.take(20))

    with open('C:\\Users\\natal\\Documents\\ADatos\\practica3\\happiness.txt',
          newline='', encoding="utf-8") as tsvfile:
        reader = csv.reader(tsvfile, delimiter='\t')
        dic_felicidad = {row[0]: row[2] for row in reader if len(row) > 2}
    bc_felicidad = sc.broadcast(dic_felicidad)

    #print(palabras_felicidad.take(20))

    palabras_por_linea = col12_rdd.map(lambda x: (x[0], x[1].split()))

    palabras_expandidas = palabras_por_linea.flatMap(
    lambda x: [(x[0], palabra) for palabra in x[1]]
    )

    emocion_por_palabra = palabras_expandidas.map(
    lambda x: (x[0], float(bc_felicidad.value.get(x[1], 0)))
    )

    count_emocion_por_palabra = emocion_por_palabra.reduceByKey(lambda a, b: a + b)

    #print(count_emocion_por_palabra.take(20))

    ordenado = count_emocion_por_palabra.sortBy(lambda x: x[1], ascending=False)

    #print(ordenado.take(20))

    return ordenado

#scispace("C:\\Users\\natal\\Documents\\ADatos\\practica3\\space.json")
felicidad("C:\\Users\\natal\\Documents\\ADatos\\practica3\\simpsons_script_lines.csv")
