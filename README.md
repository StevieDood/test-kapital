## test-kapital

- Se agrego un contenedor de Docker para facilitar la prueba.
- Para construir la imagen, se debe de tener previamente instalado Docker
- Correr el comando docker build -t spark1 .
- Esperar que se complete la construccion de la imagen
- Correr el comando docker run -it spark1 | grep -v "INFO" (esto para retirar de consola los logs de informacioan de spark y visualizar el resultado)

> Se agregaron casos de uso extra documentados en el codigo. 
