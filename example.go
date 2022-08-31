package contacto

import (
	"fmt"
	"ldb"
)


func GetContactsByIdGroup(idGrupo string) []map[string]string {

	query := fmt.Sprintf(`SELECT contacto_telefono.numero
						FROM contacto, contacto_telefono

								WHERE contacto.id_contacto = contacto_telefono.id_contacto
								  AND contacto.id_grupo = "%s"
								  AND contacto.estado = "activo"`, idGrupo)

	data := ldb.Query(query)

	return data

}

