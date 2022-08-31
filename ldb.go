package ldb

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
	_ "github.com/go-sql-driver/mysql"
)

// Db  puntero a la base de datos en caso de querer realizar consultas directas en funciones externas
var Db *sql.DB

// Mc  puntero a memcache
var Mc *memcache.Client

const DbFileConf string = "/etc/ldb.json"

func init() {
	var err error
	type Configuration struct {
		DbUserName   string
		DbPassword   string
		DbHost       string
		DbName       string
		MemcacheHost string
		MemcachePort string
	}

	//Cargar configuración
	fileConfig, _ := os.Open(DbFileConf)
	defer fileConfig.Close()
	decoder := json.NewDecoder(fileConfig)
	configuration := Configuration{}
	err = decoder.Decode(&configuration)
	PanicOnErr(err, false, "Error cargando la configuracion JSON")

	dbUserName := configuration.DbUserName
	dbPassword := configuration.DbPassword
	dbHost := configuration.DbHost
	dbName := configuration.DbName

	//Conexion de DB
	Db, err = sql.Open("mysql", dbUserName+":"+dbPassword+"@tcp("+dbHost+":3306)/"+dbName)
	PanicOnErr(err, false, "Falla al conectar con la base de datos")

	Mc = memcache.New(configuration.MemcacheHost + ":" + configuration.MemcachePort)

}

// MD5 recibe un string y retorna el respectivo hash
func MD5(data string) string {
	h := md5.Sum([]byte(data))
	return fmt.Sprintf("%x", h)
}

// Do permite realizar consultas de tipo acción, como son inserts, updates etc.
// retorna LastInsertId o rowAffected dependiendo de la consulta

func Do(query string) (int64, error) {
	result, err := Db.Exec(query)

	PanicOnErr(err, true, "Error realizando la consulta: \n"+query)

	if err != nil {
		return 0, err
	}

	if strings.Contains(strings.ToUpper(query), "INSERT") {
		lastInsertID, err := result.LastInsertId()
		return lastInsertID, err
	} else {
		rowsAffected, err := result.RowsAffected()
		return rowsAffected, err
	}

}

// cacheQuery crea el varNameCache basado en el grupoCache y hash MD5 del query y lo consulta
func cacheQuery(query string, grupoCache string) ([]byte, error) {

	var nulo []byte

	varNameCache := grupoCache + "_" + MD5(query)

	cache, err := Mc.Get(varNameCache)

	if err != nil {

		//fmt.Println("Error consultando cache: ", err)
		return nulo, err

	} else {
		//fmt.Println("Hit de cache")
		return cache.Value, err

	}

}

// cacheAdd crea el key basado en el grupoCache + el MD5 del query y almacena en memcache
func cacheAdd(query string, grupoCache string, data []byte, ttlCache int32) error {

	varNameCache := grupoCache + "_" + MD5(query)

	setItem := memcache.Item{
		Key:        varNameCache,
		Value:      data,
		Expiration: ttlCache,
	}

	err := Mc.Set(&setItem)

	PanicOnErr(err, true, "error grabando key en memcache")

	return err

}

// QueryValue recibe parametros variables : query, grupoCache, ttlCache
// realiza la consulta a la db; si se especifica grupoCache se realizara cache del resultado
// en otro caso no se realizará. si no se especifica ttlCache el valor por defecto es de una hora.
// retorna un string con el respectivo valor consultado.
func QueryValue(parametros ...string) string {
	var value string

	var query string
	var grupoCache string
	var ttlCache int32
	var err error

	//fmt.Println("Los parametros fueron: ", parametros)

	// Damos manejo a los parametros variables de la funcion
	// estableciendo los valor por defecto para cada caso

	if len(parametros) == 1 {
		query = parametros[0]
		grupoCache = ""
		ttlCache = 3600
	} else if len(parametros) == 2 {
		query = parametros[0]
		grupoCache = parametros[1]
		ttlCache = 3600
	} else if len(parametros) == 3 {
		query = parametros[0]
		grupoCache = parametros[1]
		convEntero, err := strconv.ParseInt(parametros[2], 10, 32)
		PanicOnErr(err, false, "error convirtiendo string a int32")
		ttlCache = int32(convEntero)
	}

	cacheValor, err := cacheQuery(query, grupoCache)

	if err == nil {
		return fmt.Sprintf("%s", cacheValor)
	}

	err = Db.QueryRow(query).Scan(&value)

	PanicOnErr(err, true, "error consultado el valor en la db")
	if err == nil {
		if grupoCache != "" {
			cacheAdd(query, grupoCache, []byte(value), ttlCache)
		}
		return value
	}

	return "NO_ENCONTRADO"

}

// Query recibe parametros variables : query, grupoCache, ttlCache
// realiza la consulta a la db; si se especifica grupoCache se realizara cache del resultado
// en otro caso no se realizará cache. si no se especifica ttlCache el valor por defecto es de una hora.
// retorna un slice de strings con los campos y un slice de slice de strings con los valores en una consulta total.
func Query(parametros ...string) []map[string]string {
	var query string
	var grupoCache string
	var ttlCache int32
	var err error

	//fmt.Println("Los parametros fueron: ", parametros)

	// Damos manejo a los parametros variables de la funcion
	// estableciendo los valor por defecto para cada caso

	if len(parametros) == 1 {
		query = parametros[0]
		grupoCache = ""
		ttlCache = 3600
	} else if len(parametros) == 2 {
		query = parametros[0]
		grupoCache = parametros[1]
		ttlCache = 3600
	} else if len(parametros) == 3 {
		query = parametros[0]
		grupoCache = parametros[1]
		convEntero, err := strconv.ParseInt(parametros[2], 10, 32)
		PanicOnErr(err, false, "error convirtiendo string a int")
		ttlCache = int32(convEntero)
	}

	// Definimos el  tipo que sera usado por el valor fullData como plantilla para json.Unmarshal
	type fullDataModelo struct {
		//Fields []string
		//Values [][]string
		Data []map[string]string
	}

	var fullData fullDataModelo

	// Si aplica cache lo consultamos para verificar si existe o no la clave/valor

	if grupoCache != "" {
		cacheValor, err := cacheQuery(query, grupoCache)

		if err == nil {
			err = json.Unmarshal(cacheValor, &fullData)
			if err == nil {
				//fmt.Println("Hit memcache consulta completa...!")
				//return fullData.Fields, fullData.Values
				return fullData.Data
			}

		}
	}

	rows, err := Db.Query(query)
	PanicOnErr(err, false, "error consultando la db")

	cols, err := rows.Columns() // Remember to check err afterwards
	PanicOnErr(err, false, "error obteniendo las columnas")

	//fmt.Println(cols)

	vals := make([]interface{}, len(cols))

	for i, _ := range cols {
		vals[i] = new(sql.RawBytes)
	}

	//var data [][]string
	var data []map[string]string

	for rows.Next() {
		//var item []string

		err = rows.Scan(vals...)

		mapFila := make(map[string]string)

		for i, v := range vals {
			value := fmt.Sprintf("%s", v)
			//fmt.Println(cols[i], "\t:\t", value[1:])
			//item = append(item, value[1:])

			mapFila[cols[i]] = value[1:]

		}

		data = append(data, mapFila)

	}

	// Si aplica cache entonces codificar y almacenar la clave/valor
	if grupoCache != "" {

		fullData = fullDataModelo{
			//Fields: cols,
			//Values: data,
			Data: data,
		}

		dataEncode, err := json.Marshal(fullData)

		PanicOnErr(err, false, "error decodificando json")

		cacheAdd(query, grupoCache, dataEncode, ttlCache)

	}

	return data

}

//PanicOnErr pone el error en el archivo de log (Flog) y luego genera un panic
func PanicOnErr(err error, logOnly bool, logMessage string) {
	if err != nil {

		log.Println("log message:", logMessage)
		log.Println("error:\n", err)

		if logOnly == false {
			panic(err.Error())
		}
	}
}
