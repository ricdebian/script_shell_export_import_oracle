#!/bin/ksh
FASE=AH
######
# Función que comprueba errores en los logs de los exports
######
comprueba_err(){
	    echo "Comprobando err..."
		tail -n1 $1 | grep "Export terminated successfully without warnings" 2>&1 > /dev/null
		SIN_ERRORES=$?
		tail -n1 $1 | grep "Export terminated successfully with warnings" 2>&1 > /dev/null
		CON_ERRORES=$?
		tail -n3 $1 | grep "EXP-00091" 2>&1 > /dev/null
		ERROR_91=$?
		
		echo "$SIN_ERRORES $CON_ERRORES $ERROR_91"
	    
		if [[ $SIN_ERRORES -ne 0 && $CON_ERRORES -eq 0 && $ERROR_91 -ne 0 ]]
	    then
	    # Si se ha generado un error que no es el 91 muestro el error y salgo.
	     echo "Error en el exp ${fichero}"
	      exit 1
	    fi		
	}

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
EXP=expdp
FILE=dumpfile
LOG=logfile
OTROS=direct=y 
############
## Carga de parámetros.
TABLA=$1
PROC=$2
FECHA=$3
SALIDA="/app/EXPL/E592/exports";

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


EXP_DIR="/app/EXPL/E592/exports/ejecuciones/MDG_MENSUAL"

if [ -d ${EXP_DIR} ]
then 
EXP_DIR="${EXP_DIR}/${FECHA_MDG}/ASIGNACION_HUECO_VP/${ORACLE_SID}/${PROC}/${FECHA}"
  if [ ! -d ${EXP_DIR} ]
  then 
    mkdir -p ${EXP_DIR}
    echo "Guardando en el directorio ${EXP_DIR}"
  fi
else
 echo "UDE-00000: Error. No existe el directorio de la fase."
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


echo "conn "${CONN} > tmp_insert_${TABLA}.sql
echo "
declare
v_count number;
begin
select count(*) INTO v_count 
from ${TABLA};

MERGE INTO pbtb_procesos_exports USING dual ON (tabla = '${TABLA}' and fase = '${FASE}' and paso ='${PROC}' and fecha = '${FECHA}' and fecha_mdg = '${FECHA_MDG}' )
WHEN MATCHED THEN UPDATE SET count_ini = v_count, count_fin=null
WHEN NOT MATCHED THEN INSERT (tabla,fase,paso,fecha,fecha_mdg,HORA,count_ini,count_fin) 
    values ('${TABLA}','${FASE}','${PROC}','${FECHA}','${FECHA_MDG}',to_char(sysdate,'HH24:MI:SS'),v_count,null);
commit;	
end;
/

exit" >> tmp_insert_${TABLA}.sql
sqlplus  /nolog @tmp_insert_${TABLA}.sql

#sqlplus  /nolog @insert_${TABLA}.sql

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
rm tmp_insert_${TABLA}.sql

echo "Particiones a exportar ${PARTS}";


if [ "${PARTS}" = "" ]
then
# Exportar tablas sin particionar
echo "La tabla a exportar no tiene particiones."
echo "Exportando..."

		touch ${SALIDA}/${FASE}_${TABLA}_${PROC}_${FECHA}.dmp
		chmod 666 ${SALIDA}/${FASE}_${TABLA}_${PROC}_${FECHA}.dmp
		
		echo ${CONN} | ${EXP} tables=${TABLA} $FILE=${FASE}_${TABLA}_${PROC}_${FECHA}.dmp \
		${LOG}=${FASE}_${TABLA}_${PROC}_${FECHA}.log directory=EDLEXPORT reuse_dumpfiles=y compression=all 2>/dev/null 1>/dev/null #JOB_NAME=${TABLA}_${PROC}_${FECHA}
		
		mv -f ${SALIDA}/${FASE}_${TABLA}_${PROC}_${FECHA}.* ${EXP_DIR}/
		 #direct=y
		
	    comprueba_err_exp_dp ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${FECHA}.log
	    
		gzip -f ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${FECHA}.dmp &
				
else
# Exportar tablas paricionadas
echo "La tabla a exportar está particionada."
echo "Exportando..."
	for PART in ${PARTS}
		do
	
###		${EXP} userid=${CONN} tables=${TABLA}:${PART} $FILE=${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.dmp \
      
		touch ${SALIDA}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.dmp
		chmod 666 ${SALIDA}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.dmp		
		
	    echo ${CONN} | ${EXP} tables=${TABLA}:${PART} $FILE=${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.dmp\
		${LOG}=${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.log directory=EDLEXPORT reuse_dumpfiles=y compression=all  1>/dev/null 2>/dev/null  #JOB_NAME=${TABLA}_${PROC}_${PART}_${FECHA}
		
        mv -f ${SALIDA}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.* ${EXP_DIR}/
		#direct=y

		# miro si se ha generado un error en los logs del export
		comprueba_err_exp_dp ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.log	
		 		 
		gzip -f ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_${PART}_${FECHA}.dmp &

		done

fi	

rows=0;
a=0;

# Revisa los rows insertados de los logs y actualiza la tabla de control de export
for fichero in ${EXP_DIR}/${FASE}_${TABLA}_${PROC}*${FECHA}.log 
  do
    a=`grep 'rows' $fichero | awk '{print $7}'`;
	
	if [[ $a != "" ]]
	then
	 rows=$((rows+a));
	fi
 
  done
  echo " registros totales de la tabla ${TABLA} exportados: $rows"

echo "conn "${CONN} >   tmp_update_${TABLA}.sql

echo "
WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK
SET SERVEROUTPUT ON SIZE 1000000;
SET FEEDBACK OFF;


	
update pbtb_procesos_exports
   SET COUNT_FIN = $rows
  where tabla = '${TABLA}'
    AND fase = '${FASE}'
	AND paso = '${PROC}'	
	AND fecha = '$FECHA'
	AND fecha_mdg = '${FECHA_MDG}';
commit;


declare
v_tabla pbtb_procesos_exports.tabla%type;

begin

 select tabla  
 into v_tabla
 from pbtb_procesos_exports
  where tabla = '${TABLA}'
    AND fase = '${FASE}'
	AND paso = '${PROC}'
	AND fecha = '$FECHA'
	AND fecha_mdg = '${FECHA_MDG}'	
	AND count_ini=count_fin;
exception
when no_data_found then

dbms_output.put_line('UDE-00000. Error de integridad de export. Error en la fase ${FASE} paso ${PASO} en la tabla ${TABLA}');
raise;
end;
/


exit" >> tmp_update_${TABLA}.sql

sqlplus  /nolog @tmp_update_${TABLA}.sql 2> ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_UPDATE_${FECHA}.log
if [ $? -ne 0 ];
then
   echo "Error en la fase ${FASE} paso ${PASO} en la tabla ${TABLA}'";
   cat ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_UPDATE_${FECHA}.log
   exit 1
else
  echo "successfully completed" >> ${EXP_DIR}/${FASE}_${TABLA}_${PROC}_UPDATE_${FECHA}.log;
fi

comprueba_espacio;

# borro script temporales
rm tmp_update_${TABLA}.sql

exit

	
