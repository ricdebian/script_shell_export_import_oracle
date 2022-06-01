# 19/11/2021: cambiamos la función comprueba_err por las funciones comprueba_err_exp y comprueba_espacio que hay en el fichero .sesion.dat
#!/bin/ksh

# Función comentada el 19/11/2021
######
# Función que comprueba errores en los logs de los exports
######
# comprueba_err(){
	    # echo "Comprobando err..."
		# tail -n1 $1 | grep "Export terminated successfully without warnings" 2>&1 > /dev/null
		# SIN_ERRORES=$?
		# tail -n1 $1 | grep "Export terminated successfully with warnings" 2>&1 > /dev/null
		# CON_ERRORES=$?
		# tail -n3 $1 | grep "EXP-00091" 2>&1 > /dev/null
		# ERROR_91=$?
		
		# echo "$SIN_ERRORES $CON_ERRORES $ERROR_91"
	    
		# if [[ $SIN_ERRORES -ne 0 && $CON_ERRORES -eq 0 && $ERROR_91 -ne 0 ]]
	    # then
	    # # Si se ha generado un error que no es el 91 muestro el error y salgo.
	     # echo "Error en el exp ${fichero}"
	      # exit 1
	    # fi		
	# }

####

## Nombre de parametros para utilizar la utilidad de exportación expdp en lugar de exp tradicional
###########
# EXP=expdp
# FILE=dumpfile
# LOG=logfile
# OTROS=parallel=4 
############
## Nombre de parametros para utilizar la utilidad de export tradicional exp
############
EXP=exp
FILE=file
LOG=log
OTROS=direct=y 
############
## Carga de parámetros.
TABLA=$1
PROC=$2
FECHA=$3

. /app/EXPL/E592/entrada/scripts/MDG_MENSUAL/.sesion.dat


echo "###################################################"
echo "##        Ejecución en $ORACLE_SID       		   ##"
echo "##  Con los usuarios ${ORA_USU_A} y ${ORA_USU_E} ##" 
echo "###################################################"


if [ $# -ne 3 ];
then
  echo "Error en el nº de parametros, la llamada al script ksh debe ser export_calcula_partitions.ksh tabla proceso fecha"
  exit 1
fi

FASE=AH

EXP_DIR="/app/EXPL/E592/exports/ejecuciones/MDG_MENSUAL/ASIGNACION_HUECO_VP"
if [ -d ${EXP_DIR} ]
then 
EXP_DIR="${EXP_DIR}/${ORACLE_SID}/${PROC}/${FECHA}"
  if [ ! -d ${EXP_DIR} ]
  then 
    mkdir -p ${EXP_DIR}
    echo "Guardando en el directorio ${EXP_DIR}"
  fi
else
 echo Error. No existe el directorio de la fase.
 exit 1  
fi

####
# Genero script sql que mira las particiones y las guarda en un shell unix


CONN="${ORA_USU_E}/${ORA_PWD_E}@${ORACLE_SID}"

echo "conn "${CONN} > temp${TABLA}_PART.sql
echo "set heading off
set pagesize 0
set feedback off
set termout off
set linesize 100
set trims on
set heading off
spool particiones${TABLA}.ksh

SELECT 
'PARTS='||CHR(34)||'$'||'{PARTS}'||PARTITION_NAME||' '||CHR(34)
from sys.all_tab_partitions
where table_name='${TABLA}'
/
spool off

exit" >> temp${TABLA}_PART.sql

sqlplus  /nolog @temp${TABLA}_PART.sql



if [ -f particiones${TABLA}.ksh  ]
 then
 echo "cargo particiones${TABLA}.ksh..."
# Cargo las particionoes que tenga la tabla con el script generado por el sql. 
. particiones${TABLA}.ksh
fi
# Borro los script temporales que cargan las particiones.
echo "Borro script temporales..."
rm particiones${TABLA}.ksh
rm temp${TABLA}_PART.sql 


echo "Particiones a exportar ${PARTS}";


if [ "${PARTS}" = "" ]
then
# Exportar tablas sin particionar
echo "La tabla a exportar no tiene particiones."
echo "Exportando..."
 echo ${CONN} | ${EXP} tables=${TABLA} $FILE=${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${FECHA}.dmp \
	  ${LOG}=${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${FECHA}.log ${OTROS}
		 
		 #direct=y
		
	    # Modificado el 19/11/2021
		# comprueba_err ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${FECHA}.log
		comprueba_err_exp ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${FECHA}.log
	    comprueba_espacio
		
		gzip -f ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${FECHA}.dmp &
else
# Exportar tablas paricionadas
echo "La tabla a exportar está particionada."
echo "Exportando..."
	for PART in ${PARTS}
		do
	
###		${EXP} userid=${CONN} tables=${TABLA}:${PART} $FILE=${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.dmp \

	    echo ${CONN} | ${EXP} tables=${TABLA}:${PART} $FILE=${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.dmp \
		${LOG}=${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.log ${OTROS}

		#direct=y

		# miro si se ha generado un error en los logs del export
		# Modificado el 19/11/2021		
		#comprueba_err ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.log
		comprueba_err_exp ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.log	
	    comprueba_espacio
		
		gzip -f ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.dmp &

		done

fi	


exit

	
