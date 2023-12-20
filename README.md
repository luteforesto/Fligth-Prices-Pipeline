# Flight_Price_Analysis
Data Engineering Project for Coderhouse

**Descripción del Proyecto**

En este trabajo se realizara un proyecto de ingenieria de datos en donde obtendremos datos de una API Pública y los guardaremos en un dataframe, para luego ingresarlos a una base de datos en Redshift. Para la eleccion de la Api a trabajar realizamos una investigación por diferentes paginas con acceso público, tomando como principal objetivo que sea un tema que me guste y me motive. Principalmente busque en APIs que esten relacionadas con el turismo, ya que es un tema que me gusta mucho y entendemos que podria ser muy útil para este trabajo. Dimos con la API de Amadeus que es una empresa tecnológica especializada en la industria de los viajes y el turismo que ofrecen una amplia gama de soluciones tecnológicas para empresas y organizaciones en el sector de viajes, como aerolíneas, hoteles, agencias de viajes, operadores turísticos y más. Con simplemente registrarse en su web se puede acceder a varios endpoints de manera gratuita y con pocas limitaciones. El endpoint elegido fue "Fight Price Analysis" que con unos determinados parámetros te permite obtener el rango de precios de vuelos para entender si estamos consiguiendo un buen deal o si es una buena oferta. Para este primer entregable, haremos solo una sencilla busqueda desde un origen fijo y con una fecha estipulada (Madrid - 15/11/2023) con algunos destinos posibles, para entender como funciona el request y dar una muestra de la información que podemos obtener. Más adelante, se pueden agregar muchos otros destinos, distintos origenes y posibles fechas.

**API**

Para poder trabajar con esta API, debimos crearnos una cuenta en la web de Amadeus donde nos otorgo un api_key y api_secret. Estas las almacenamos en un archivo config.ini y las ultilizamos para hacer un post request, para poder obtener un token y de esta forma hacer las solicitudes. La API cuenta con tres parámetros obligatorios: -originIataCode -destinationIataCode -departureDate Y también cuenta con otros dos parámetros que no son obligatorios, pero que se puede utilizar para limitar o ampliar la búsqueda: -currency: tipo de moneda -oneway: si es un ticket solo de ida, o ida y vuelta. Una vez solicitado el token con nuestra credenciales, podemos hacer la solicitud a la API para ver los resultados. En nuestro caso tenemos que iterar sobre cada uno de los posibles destino para ir haciendo una solicitud por tramo, y obtenemos para cada uno el rango de precios en cuartiles. Tuvimos que hacer algunos retoques de la info original, ya que venian en json anidados y también producia una fila por cada cuartil de cada tramo, haciendolo un poco redundante.

**Conexión Base de Datos y Tabla en Redshift**

La segunda parte de este entregable, consta de crear una conexión a Redshift con nuestras credenciales y crear una tabla en donde luego se almacenaran los datos que obtenemos de nuestra API. Para esto crearemos una función para conectarnos a Redshift, en nuestro archivo config.ini ya tenemos almacenadas nuestras credenciales en otra sección. Luego, creamos la tabla con la definición de cada tipo de dato para cada columna. Una de las cosas mas importantes en esta etapa es la definición de una distribution key y de una sort key.

DDL <br/>
origin: el origen del vuelo, se ira iterando entre varias opciones. <br/>
destination: destino del vuelo al que le solicitaremos el rango de precio. <br/>
departureDate: fecha de partida <br/>
currencyCode: código que indica la moneda en que se mostrara el precio de los vuelos. <br/>
oneWay: columna de tipo booleana para indicarnos si es un vuelo ida y vuelta o solo ida. <br/>
MINIMUM: precio mínimo para ese tramo. <br/>
FIRST: precio que nos indica el primer cuartil para ese tramo. <br/>
MEDIUM: precio medio para ese tramo. <br/>
THIRD: precio que nos indica el tercer cuartil para ese tramo. <br/>
MAXIMUM: precio máximo para ese tramo <br/>

Dist Key: Utilizaremos columna origin (origen de los vuelos), ya que entendemos que va a ser usado con frecuencia en clausulas de filtrado y operaciones de JOIN una vez que haya un número muy grande de pasajes en nuestras tablas. <br/>

Sort Key: Utilizaremos la columna departureDate (fecha de partida de los vuelos), ya que lo vemos como un campo muy probable a usar en clausulas WHERE y Order By. Usar la fecha en que estamos buscando los vuelos para ver su rango, podria acelerar de buena manera las consultas para ser ordenadas. <br/>


Por último, utilizaremos el método .to_sql para cargar a Redshift toda la información de los vuelos que obtuvimos en nuestro dataframe con la solicitud a la API. Creamos dos tablas, una de Stagging y otra de Dimensión. La tabla de Stagging (stg) la utilizaremos para almacenamiento temporal, para ir metiendo los datos crudos antes de realizar transformaciones y cargarlos a la tabla final. La tabla de Dimension (dim_flight_prices) sera nuestra tabla consolidada final en donde tendremos nuestros datos únicos e iremos poniendo toda la data de distintas solicitudes evitando duplicados. Esto lo lograremos con la sentencia MERGE, en donde para cada registro que tenga un mismo Origen, Destino y Fecha se reemplazara si hay uno nuevo. Para esto definimos como Primary Key Compuesta las tres columnas (origin, destination, departureDate). Es importante destacar que para esta entrega como hubo una sola petición ambas tablas tienen la misma información, pero a medida que vayamos haciendo mas solicitudes a la API, esto nos servira para añadir nuevos registros y actualizar los existentes evitando duplicados.

**Container de Docker y DAG en Airflow**

Para la tercera entrega, lo que hicimos fue crear un conjunto de contenedores en Docker para embeber en un DAG en Airflow. Para esto creamos un DAG con todo lo necesario para correr la función (load_flight_prices_data) a través de un PythonOperator el cual nos permitira correr el script y sacar los datos de vuelos de la API. De esta forma podemos coordinar nuestras tasks y monitorear nuestro flujo de trabajo a través de Airflow. 

