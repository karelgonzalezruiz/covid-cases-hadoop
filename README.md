# COVID-19 Cases con Hadoop MapReduce

Implementación en **Java** para el procesamiento de casos positivos de COVID-19 por país mediante **Apache Hadoop MapReduce**, utilizando **HDFS** para el almacenamiento distribuido de datos y **YARN** para la administración de recursos y ejecución del trabajo.

El proyecto procesa el dataset `owid-covid-data.csv` de *Our World in Data* con el objetivo de obtener el último total acumulado de contagios disponible para cada país, evitando duplicaciones derivadas de sumar registros que ya contienen cantidades acumuladas.

## Objetivo

Procesar un archivo CSV mediante Hadoop MapReduce y obtener el número total de casos positivos de COVID-19 registrados por país, aplicando las siguientes reglas:

- Identificar el país mediante la columna `location`.
- Utilizar la columna `total_cases`, que contiene los casos acumulados.
- Seleccionar para cada país el valor correspondiente a la fecha más reciente con información válida.
- Excluir registros correspondientes a agrupaciones geográficas, continentes y regiones.
- Omitir registros con valores vacíos o inválidos.
- Generar un archivo de salida con las columnas `pais,total_casos`.

**Ejemplo de salida:**

```csv
pais,total_casos
Afghanistan,174331
Albania,272030
Ecuador,836216
United States,79265726
```

Los valores corresponden a los últimos registros acumulados disponibles en el dataset utilizado, no a estadísticas actuales.

**Consideración:** no se suman los valores de `total_cases` porque cada registro ya contiene el acumulado hasta una determinada fecha. Sumarlos provocaría contabilizar repetidamente los mismos contagios.

## Tecnologías utilizadas

- **Java:** configuración de compilación con compatibilidad Java 8.
- **Apache Hadoop 3.5.0:** procesamiento distribuido mediante MapReduce.
- **HDFS:** almacenamiento de los archivos de entrada y salida.
- **YARN:** administración de recursos y ejecución de tareas.
- **Apache Maven:** compilación y empaquetado del proyecto.
- **IntelliJ IDEA:** entorno de desarrollo.
- **Ubuntu sobre WSL2:** entorno utilizado para la ejecución de Hadoop.
- **CSV:** formato de entrada y exportación de resultados.

## Estructura del repositorio

```text
covid-cases-hadoop/
├── .gitignore
├── README.md
├── pom.xml
├── src/
│   └── main/
│       └── java/
│           └── org/
│               └── karel/
│                   ├── CovidMapper.java
│                   ├── CovidReducer.java
│                   └── CovidRunner.java
├── datos/
│   └── owid-covid-data.csv
├── resultados/
│   ├── resultado_covid.csv
│   └── resultado_mapreduce.txt
└── evidencias/
    ├── 01_Estructura_Proyecto_y_JAR.png
    ├── 02_Carga_Dataset_COVID_HDFS.png
    ├── 03_Ejecucion_MapReduce_Exitosa.png
    ├── 04_Contadores_MapReduce_215_Paises.png
    ├── 05_Exportacion_y_Validacion_CSV.png
    └── 06_Exportacion_TXT_y_Verificacion.png
```

Las carpetas `.idea/` y `target/` se excluyen del repositorio mediante `.gitignore`, debido a que contienen configuraciones locales y archivos generados automáticamente. El JAR puede reconstruirse mediante Maven.

## Dataset utilizado

El proyecto utiliza el dataset `owid-covid-data.csv` de **Our World in Data**, con registros históricos de la pandemia de COVID-19.

Las principales columnas utilizadas son:

| Columna | Descripción |
|---|---|
| `continent` | Continente al que pertenece el país |
| `location` | Nombre del país |
| `date` | Fecha del registro |
| `total_cases` | Total acumulado de casos confirmados |

El archivo contiene **166.326 registros de datos**, sin contar el encabezado, y su fecha máxima es el **5 de marzo de 2022**.

No todos los países necesariamente tienen un registro válido en esa fecha. Por ello, el programa selecciona el último valor disponible de cada país.

Fuente del dataset: [Our World in Data - COVID-19](https://ourworldindata.org/coronavirus).

## Funcionamiento del programa

### Mapper (`CovidMapper.java`)

El Mapper recibe cada línea del CSV y extrae la información necesaria para el procesamiento.

Primero identifica el país, la fecha y el total acumulado de contagios. Posteriormente, descarta los registros que no corresponden a países o que contienen datos inválidos.

Para cada registro válido, emite un par clave-valor donde:

- **Clave:** nombre del país.
- **Valor:** fecha y número de casos acumulados.

Ejemplo:

```text
Ecuador    2022-03-03|833777
Ecuador    2022-03-05|836216
```

Esto permite que Hadoop agrupe todos los registros pertenecientes a un mismo país antes de enviarlos al Reducer.

### Reducer (`CovidReducer.java`)

El Reducer recibe los registros agrupados por país y busca la fecha más reciente que tenga un valor válido en `total_cases`.

La comparación se realiza utilizando las fechas en formato `YYYY-MM-DD`, que permite establecer su orden cronológico.

Por cada país se genera un único resultado con el último total acumulado disponible.

Ejemplo:

```csv
Ecuador,836216
```

De esta manera, se evita sumar valores acumulados de diferentes fechas, lo cual produciría resultados incorrectos.

### Runner (`CovidRunner.java`)

La clase Runner configura y ejecuta el trabajo MapReduce.

Sus principales responsabilidades son:

- Registrar las clases Mapper y Reducer.
- Configurar los tipos de claves y valores.
- Establecer las rutas de entrada y salida en HDFS.
- Utilizar una coma como separador de los resultados.
- Configurar un único Reducer para generar un archivo de salida.
- Ejecutar el trabajo y verificar su finalización.

## Compilación y ejecución

**Requisitos:** Java y Maven configurados, además de una instalación funcional de Apache Hadoop con HDFS y YARN iniciados.

### 1. Compilar el proyecto

Desde la carpeta principal del proyecto:

```bash
mvn clean package
```

Maven genera el archivo:

```text
target/TallerMapReduce-1.0-SNAPSHOT.jar
```

La compilación se realizó satisfactoriamente mediante IntelliJ IDEA, obteniendo el mensaje `BUILD SUCCESS`.

### 2. Cargar el dataset en HDFS

Crear el directorio de entrada:

```bash
hdfs dfs -mkdir -p /user/$USER/covid_input
```

Subir el archivo CSV:

```bash
hdfs dfs -put -f datos/owid-covid-data.csv /user/$USER/covid_input/
```

Comprobar que el archivo se encuentra en HDFS:

```bash
hdfs dfs -ls -h /user/$USER/covid_input/
```

### 3. Ejecutar Hadoop MapReduce

```bash
hadoop jar target/TallerMapReduce-1.0-SNAPSHOT.jar \
org.karel.CovidRunner \
/user/$USER/covid_input/owid-covid-data.csv \
/user/$USER/covid_salida
```

Hadoop ejecuta el Mapper y el Reducer mediante YARN, procesando el dataset almacenado en HDFS.

**Importante:** el directorio de salida `/user/$USER/covid_salida` no debe existir antes de iniciar el trabajo.

### 4. Consultar los resultados en HDFS

```bash
hdfs dfs -ls /user/$USER/covid_salida/
```

La ejecución generó los siguientes archivos:

```text
_SUCCESS
part-r-00000
```

El archivo `_SUCCESS` indica que el trabajo terminó correctamente y `part-r-00000` contiene los resultados generados por el Reducer.

Para consultar los primeros registros:

```bash
hdfs dfs -cat /user/$USER/covid_salida/part-r-00000 | head
```

### 5. Exportar los resultados a CSV y TXT

Crear la carpeta local de resultados:

```bash
mkdir -p resultados
```

Descargar el resultado como CSV:

```bash
hdfs dfs -get -f /user/$USER/covid_salida/part-r-00000 resultados/resultado_covid.csv
```

Descargar el resultado original como TXT:

```bash
hdfs dfs -get -f /user/$USER/covid_salida/part-r-00000 resultados/resultado_mapreduce.txt
```

Ambos archivos contienen los mismos datos, utilizando una coma como separador.

## Resultados obtenidos

La ejecución del trabajo MapReduce terminó exitosamente, procesando los registros históricos del dataset y obteniendo un resultado por país.

| Indicador | Resultado |
|---|---:|
| Registros de entrada, incluido el encabezado | **166.327** |
| Registros emitidos por el Mapper | **153.344** |
| Grupos procesados por el Reducer | **215** |
| Países con resultados | **215** |
| Líneas del archivo final, incluido el encabezado | **216** |
| Tareas Mapper ejecutadas | **1** |
| Tareas Reducer ejecutadas | **1** |
| Errores de Shuffle | **0** |

Algunos resultados obtenidos fueron:

| País | Casos acumulados |
|---|---:|
| Afghanistan | 174.331 |
| Albania | 272.030 |
| Ecuador | 836.216 |
| United States | 79.265.726 |

Los archivos generados están disponibles en:

- [Resultado CSV](resultados/resultado_covid.csv)
- [Resultado TXT](resultados/resultado_mapreduce.txt)

### Verificación de resultados

La cantidad total de líneas se comprobó mediante:

```bash
wc -l resultados/resultado_covid.csv
```

Resultado:

```text
216 resultados/resultado_covid.csv
```

También se compararon ambos archivos:

```bash
cmp resultados/resultado_covid.csv resultados/resultado_mapreduce.txt
```

El comando no produjo diferencias, confirmando que ambos archivos contienen exactamente la misma información.

Adicionalmente, se verificó que los registros tuvieran dos columnas separadas por comas.

## Evidencias

### 1. Estructura del proyecto y archivo JAR

![Estructura del proyecto](evidencias/01_Estructura_Proyecto_y_JAR.png)

### 2. Carga del dataset en HDFS

![Carga del dataset COVID-19](evidencias/02_Carga_Dataset_COVID_HDFS.png)

### 3. Ejecución exitosa de Hadoop MapReduce

![Ejecución MapReduce](evidencias/03_Ejecucion_MapReduce_Exitosa.png)

### 4. Contadores y resultados del procesamiento

![Contadores MapReduce](evidencias/04_Contadores_MapReduce_215_Paises.png)

### 5. Exportación y validación del archivo CSV

![Validación del CSV](evidencias/05_Exportacion_y_Validacion_CSV.png)

### 6. Exportación del TXT y comparación de resultados

![Verificación del TXT](evidencias/06_Exportacion_TXT_y_Verificacion.png)

## Conclusión

Mediante Apache Hadoop MapReduce se logró procesar el dataset histórico de COVID-19 y obtener los últimos casos acumulados disponibles para **215 países**, generando un archivo de resultados con **216 líneas**, incluyendo su encabezado.

La implementación permitió aplicar las etapas de mapeo, agrupación y reducción sobre un conjunto de datos real, utilizando HDFS para el almacenamiento y YARN para la ejecución del procesamiento.

Finalmente, los resultados fueron exportados en formatos CSV y TXT, y se verificó que ambos archivos fueran idénticos.

## Nota

Este repositorio documenta una **actividad académica de la asignatura Procesamiento Masivo de Datos**. Los resultados corresponden al periodo cubierto por el dataset proporcionado y no representan el número actual de contagios de COVID-19.

Para reproducir el procesamiento se requiere una instalación funcional de Apache Hadoop; no basta con ejecutar las clases Java como una aplicación convencional.
