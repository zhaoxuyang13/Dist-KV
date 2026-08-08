

##  KV Store Distribuido

### TODOs

- [x] Añadir Yml para la configuración de ZK, https://github.com/go-yaml/yaml (aunque podría ser redundante con zk-cluster.yml)
- [x] Cambiar yml a json, para consistencia
- [x] Slave - Registro del servicio.
- [x] Slave - APIs RPC Put, Del, Get
- [X] Slave - Registro RPC
- [x] Client - Una interfaz REPL
- [x] Master - Interfaz de Unirse/Salir/Consultar
- [x] Client - Selección automática de primary/backup al inicio.
- [x] Master - Lógica de registro
- [x] Client - Put, Del, Get, Interfaz CMDLINE, llamada RPC (una vez que el master esté listo)
- [x] Client - Manejo de valores no existentes en Del/Get, manejo elegante de otras fallas, actualización de configuración cuando la clave no pertenece 
- [x] Master/Slave/Client - Cómo migrar shards ante un cambio de configuración.
- [ ] Copiar pruebas de 6.824 / o escribir las propias
- [ ] Master/Slave/Client - Manejo de errores de red, filtrado de RPCs duplicados
- [x] Biblioteca de Locks (para acceso a datos por slaves, no es necesario)
- [x] Lógica de reelección de primary y slave backup ante fallos
- [x] Lógica normal de slave backup y primary
- [x] Script de inicio para el clúster slave-master 


**Características**
- [x] Múltiples nodos backup
- [ ] Backup para el master

### Compilación y Ejecución

**zookeeper** 

```shell
$ docker-compose -f zk-cluster up
```

- [x] Desplegar zkper mediante script propio

**Slave**

```bash
$ go run go/slave_server.go [ip] [port] [hostname] [groupID] 
# ip and port expose as RPC service
# hostname is used to distinguish
# groupID  is for grouping.
```

**Master**

```
$ go run go/master_server.go [ip] [port] [hostname]
```

**Client** 

```
$ go run go/client.go
```

**Admin**

```
$ go run go/admin.go [command] [groupIDs ...]
# command = "join-group" / "leave-group"
# groupIDs is an array of groupid to be configured
```


---
- Casos extraños 

    - Cuando el primary falla y vuelve a ser backup, la solicitud del client debe fallar. (solucionado)
