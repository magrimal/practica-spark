import json
import pprint
import sys
from gensim.utils import tokenize

from pyspark.sql import SparkSession
import csv
from pyspark.sql.functions import col


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

def felicidad(filename): #TODO: NO SE DEBERÍA COGER LA PRIMERA FILA. Creo que por parámetro se deberían pasar los ficheros.
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

def simpsons():#todo: no coger la primera fila. Creo que por parámetro se deberían pasar los ficheros.
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    rddLines = sc.textFile("C:\\Users\\natal\\Documents\\ADatos\\practica3\\simpsons_script_lines.csv")
    rddLocations = sc.textFile("C:\\Users\\natal\\Documents\\ADatos\\practica3\\simpsons_locations.csv")
    rddEpisodes = sc.textFile("C:\\Users\\natal\\Documents\\ADatos\\practica3\\simpsons_episodes.csv")
    rddCharacters = sc.textFile("C:\\Users\\natal\\Documents\\ADatos\\practica3\\simpsons_characters.csv")

    def parse_csv(line):
        try:
            return next(csv.reader([line]))
        except:
            return []
        
    parsedLines = rddLines.map(parse_csv)
    parsedLocations = rddLocations.map(parse_csv)
    parsedEpisodes = rddEpisodes.map(parse_csv)
    parsedCharacters = rddCharacters.map(parse_csv)

    parsedLines = parsedLines.filter(lambda row: len(row) > 12)# and row[5].lower() == 'true') #la columna 6 indica si es diálogo
    parsedLocations = parsedLocations.filter(lambda row: len(row) > 2)
    parsedEpisodes = parsedEpisodes.filter(lambda row: len(row) > 13)
    parsedCharacters = parsedCharacters.filter(lambda row: len(row) > 3 and row[3] != '')

    col_rdd_lines = parsedLines.map(lambda row: (row[1], (row[5], row[6], row[11])))#episode_id, speaking_line, character_id, normalized_text
    col_rdd_episodes = parsedEpisodes.map(lambda row: (row[0], row[2]))#id, imdb_raiting
    col_rdd_characters = parsedCharacters.map(lambda row: (row[0], row[3]))#id, gender
    #print("-")
    #print(col_rdd_characters.take(10))

    #JOIN DE LÍNEAS CON EPISODIOS
    rdd_lines_episodes_joined = col_rdd_lines.join(col_rdd_episodes)#(episode_id, ((speaking_line, character_id, normalized_text), (imdb_raiting))
    rdd_lines_episodes_joined = rdd_lines_episodes_joined.map(lambda row: (row[1][0][1], (row[0], row[1][0][0], row[1][0][2], row[1][1])))#(character_id, (episode_id, speaking_line, normalized_text, imdb_raiting))
    #print("-")
    #print(col_rdd_lines.take(10))
    #print("-")
    #print(col_rdd_episodes.take(10))
    # print("-")
    # print(rdd_lines_episodes_joined.take(10))

    #JOIN EPISODIOS CON CARACTERES
    rdd_lines_episodes_joined_characters_joined = rdd_lines_episodes_joined.join(col_rdd_characters)#(character_id, ((episode_id, speaking_line, normalized_text, imdb_raiting), (gender)))
    
    #print("-")
    #print(rdd_lines_episodes_joined_characters_joined.take(10))

    #TAREA 1

    tareauno = rdd_lines_episodes_joined_characters_joined.map(lambda row: (row[1][0][0], row[1][0][3], row[0], row[1][1]))#(episode_id, imdb_raiting, character_id, gender)
    tareauno = tareauno.filter(lambda row: row[3] == 'f') #personajes femeninos
    tareauno = tareauno.map(lambda x: ((x[0], x[1], x[2])))#(episode_id, imdb_raiting, character_id)
    tareauno = tareauno.distinct()
    tareauno = tareauno.map(lambda row: ((row[0], row[1]), 1))#((episode_id, imdb_raiting), 1)
    tareauno = tareauno.reduceByKey(lambda a, b: a + b)
    #print(tareauno.take(10))

    df_tarea1 = spark.createDataFrame(
        tareauno.map(
            lambda x: (x[0][0], x[0][1], x[1])
        ),
        ["episode_id", "imdb_raiting", "numero_personajes_femeninos"]
    )

    #print(df.take(10))

    #TAREA 2

    tareados = rdd_lines_episodes_joined.map(lambda row: (row[1][0], row[1][1], row[1][2], row[1][3]))#(episode_id, speaking_line, normalized_text, imdb_raiting)
    
    # print('-')
    # print(tareados.take(10))
    
    tareados = tareados.filter(lambda row: row[1].lower() == 'true') #son diálogos
    # print('-')
    # print(tareados.take(10))

    tareados = tareados.flatMap(
    lambda x: [((x[0], x[3]), 1) for palabra in x[2].split()]
    ) #episode_id, imdb_raiting, 1

    tareados = tareados.reduceByKey(lambda a, b: a+b)

    # print('-')
    # print(tareados.take(10))

    df_tarea2 = spark.createDataFrame(
    tareados.map(lambda x: (x[0][0], x[0][1], x[1])),
    ["episode_id", "imdb_rating", "total_palabras_dialogo"]
    )

    # print('-')
    # print(df_tarea2.take(10))

    #TAREA 3

    df_tarea1 = df_tarea1.withColumn("imdb_raiting", col("imdb_raiting").cast("float"))
    df_tarea2 = df_tarea2.withColumn("imdb_rating", col("imdb_rating").cast("float"))

    # Pearson 1: IMDb vs número de personajes femeninos
    pearsonTareaUno = df_tarea1.corr("imdb_raiting", "numero_personajes_femeninos")
    print("Correlación IMDb vs personajes femeninos:", pearsonTareaUno)

    # Pearson 2: IMDb vs número total de palabras
    pearsonTareaDos = df_tarea2.corr("imdb_rating", "total_palabras_dialogo")
    print("Correlación IMDb vs total de palabras:", pearsonTareaDos)

    return rdd_lines_episodes_joined_characters_joined

#scispace("C:\\Users\\natal\\Documents\\ADatos\\practica3\\space.json")
#felicidad("C:\\Users\\natal\\Documents\\ADatos\\practica3\\simpsons_script_lines.csv")
simpsons()
