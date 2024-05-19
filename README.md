# Scrapper para la página HowLongToBeat
---
## Proposito del proyecto
Con este proyecto pretendo conseguir la información sobre la duración de los juegos que luego utilizaré para mi proyecto principal.

---
## Scrappeo de la página
- Comprobamos que tenemos instalado Python 3.12 o superior en nuestro equipo.
- Creamos un entorno virtual de python con `python -m venv .\env`. (Esto es totalmente opcional, pero recomendado)
- Comenzamos el proyecto con `.\env\Scripts\activate`.
- Desde este entorno ejecutamos `pip install -r requirements-linux.txt` o `pip install -r requirements-windows.txt` dependiendo de nuestro sistema operativo.
- Finalmente, ejecutamos el programa con `python Scrapper.py`
Esto se tomara un tiempo bastante largo ya que son muchas entradas lo que conseguirá asi que no recomiendo ejecutarlo constantemente.
Si en algun momento se parara el programa o decidieramos cancelarlo para continuar mas adelante, podriamos indicarle desde donde continuar simplemente cambiando la variable `start`

---
## Procesado de datos
Para el procesado de datos he decidido utilizar databricks como framework para ello. Simplemente con una cuenta gratuita de Databricks community o una cuenta de Azure/AWS que posea un recurso de este, podriamos ejecutar el archivo `DatabricksProcessing.py`