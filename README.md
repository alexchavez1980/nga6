# Seminario de Tópicos Avanzados en Datos Complejos
forked from https://github.com/arjones/bigdata-workshop-es


Docente:   
Pedro Ferrari | pedro@muttdata.ai  
Juan Martín Pampliega | jpamplie@itba.edu.ar  

Estudiantes:    

Guillermo Lencina | glencina@itba.edu.ar    
Nicolas Arosteguy | narosteguy@itba.edu.ar    
Alexander Chavez | achavezmontano@itba.edu.ar   


## Pasos para instalar todo y ver avances:

1. Abrir _Docker Desktop_ y tener la interfaz para ver los resultados. Acá no hay que hacer nada.
2. Abrir WSL, luego la consola de Ubuntu.
3. Ejecutar/Descargar todo el proyecto: git clone https://github.com/alexchavez1980/nga5.git
4. Ingresar: cd nga
5. Ejecutar: docker-compose -f <archivo.yml> up -d. Ejemplo: docker-compose -f dc10.yml up -d

* El docker-compose es un archivo yaml/yml para crear todos los containers necesarios y a la vez.
* Para borrar todos los containers, incluso las redes: docker compose down
* Para conocer las redes: docker network ls. 
* Para conocer qué IPs fue asignadas a cada uno de los containers dentro de la red: 
    docker network inspect <nombre de la red>

Sitios donde miré tema Dockers: 

    * https://www.youtube.com/c/PeladoNerd  
    * https://www.youtube.com/c/HolaMundoDev  
    * https://www.youtube.com/c/NetworkChuck
    * Postgres + PGAdmin : https://www.youtube.com/watch?v=uKlRp6CqpDg  

