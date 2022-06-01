# Script que realiza la importación de tablas particionadas.
# En el parametro 1 se especifica el nombre de la tabla a importar
# En el parametro 2 se especifica el paso en el que se realizó el export del fichero que queremos importar.
# En el parametro 3 se especifica la fecha de exportación del fichero que queremos importar.
TABLA=$1
PROC=$2
FECHA=$3


. /app/EXPL/E592/entrada/scripts/MDG_MENSUAL/.sesion.dat

echo "###################################################"
echo "##        Ejecución en $ORACLE_SID               ##"
echo "##  Con los usuarios ${ORA_USU_A} y ${ORA_USU_E} ##" 
echo "###################################################"

SALIDA="/app/EXPL/E592/exports";

# Prefijo utilizado por sistemas en la copia masiva de fichero para backups.
PREF="AH"


EXP_DIR="/app/EXPL/E592/exports/ejecuciones/MDG_MENSUAL/${FECHA_MDG}/ASIGNACION_HUECO_VP/$ORACLE_SID/$PROC/$FECHA"

DIR=`pwd`
echo `basename $DIR`
#exit 1

i=0

CONN="$ORA_USU_E/$ORA_PWD_E@${ORACLE_SID}"

echo  "Ficheros a importar: ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_*${FECHA}.dmp.gz"

for FILE in `ls ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_*${FECHA}.dmp.gz`
do
    if [ -f $FILE ]
	then
	 echo ${FILE}
	 i=$(($i+1))
	fi
	
done

if [ $i -eq 0 ]
then
 echo "No existen fichero dmp de importación para la tabla $TABLA en esta fase"
 exit 1
fi


echo "Nº de particiones a importar $i"

echo "conn "${CONN} > temp${TABLA}.sql


echo "set heading off
set pagesize 0
set feedback off
set termout off
set linesize 100
set trims on

variable tab_name varchar2(30);
begin
:tab_name:='&1';
end;
/

spool trunc_drop_index${TABLA}.sql" >> temp${TABLA}.sql

echo " select 'conn ${CONN}' from dual;
select 'drop table '||:tab_name||';' from sys.all_tables where table_name=''||:tab_name||'';
select 'exit' from dual;
select '   ' from dual;

spool off

exit" >> temp${TABLA}.sql

echo "conn "${CONN} > tmp_imp_insert_${TABLA}.sql

echo "
declare
v_count number;
begin
select count_fin
  INTO v_count 
from pbtb_procesos_exports
where tabla ='${TABLA}'
 and fase = '${PREF}'
 and paso = '${PROC}'
 and fecha_mdg = '${FECHA_MDG}'
 and fecha = '${FECHA}';

MERGE INTO pbtb_procesos_imports USING dual ON (tabla = '${TABLA}' and fase = '${PREF}' and paso ='${PROC}' and fecha = '${FECHA}' and fecha_mdg = '${FECHA_MDG}' )
WHEN MATCHED THEN UPDATE SET count_ini = v_count, count_fin=null
WHEN NOT MATCHED THEN INSERT (tabla,fase,paso,fecha,fecha_mdg,HORA,count_ini,count_fin) 
    values ('${TABLA}','${PREF}','${PROC}','${FECHA}','${FECHA_MDG}',to_char(sysdate,'HH24:MI:SS'),v_count,null);
commit;
end;
/

exit" >> tmp_imp_insert_${TABLA}.sql

sqlplus  /nolog @tmp_imp_insert_${TABLA}.sql

sqlplus  /nolog @temp${TABLA}.sql ${TABLA}

sqlplus /nolog @trunc_drop_index${TABLA}.sql
echo "Borro script temporales."
rm trunc_drop_index${TABLA}.sql
rm temp${TABLA}.sql
rm tmp_imp_insert_${TABLA}.sql
echo $i

if [ $i -gt 1 ]
then
	echo "Tabla particionada"

	for FILE in `ls ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_*${FECHA}.dmp.gz`
	do
	 gunzip ${FILE}
	 BFILE=`basename $FILE`
	 echo "fichero $BFILE"
	 # Tomo la partición
	 PART=`echo "$BFILE" | awk  'BEGIN { FS="_"} {print $((NF-1))}'`
	 	
	 echo "Importando tabla $TABLA partición $PART"
	 
	 # Quito la extensión .gz del nombre del fichero a importar.
	
	 FILEDMP=`ls ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_${PART}_${FECHA}.dmp`
	 	 
	 echo $FILEDMP
	 
	 mv ${FILEDMP} ${SALIDA}/
	
	 
	 echo ${CONN} | impdp tables=${TABLA}:${PART} dumpfile=`basename ${FILEDMP}` logfile=imp_${TABLA}_${PROC}_${PART}_$(date +%G%m%d).log  directory=EDLEXPORT table_exists_action=append  1>/dev/null 2>/dev/null 
	 
	 comprueba_err_imp_dp ${SALIDA}/imp_${TABLA}_${PROC}_${PART}_$(date +%G%m%d).log
	 mv -f ${SALIDA}/`basename ${FILEDMP}` ${EXP_DIR}/
	 mv -f ${SALIDA}/imp_${TABLA}_${PROC}_${PART}_$(date +%G%m%d).log ${EXP_DIR}
	 gzip ${FILEDMP} &

	done
	
else	

	echo "No tiene particiones"
	gunzip  ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_${FECHA}.dmp.gz
	FILEDMP=`ls ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_${FECHA}.dmp`
	
	mv ${FILEDMP} ${SALIDA}/
	 
#	imp userid=${CONN} tables=${TABLA} file=${FILEDMP} log=${EXP_DIR}/imp_${TABLA}_$(date +%G%m%d).log ignore=y indexes=y
    echo ${CONN} | impdp tables=${TABLA} dumpfile=`basename ${FILEDMP}` logfile=imp_${TABLA}_${PROC}_$(date +%G%m%d).log directory=EDLEXPORT table_exists_action=append  1>/dev/null 2>/dev/null 
	
	comprueba_err_imp_dp ${SALIDA}/imp_${TABLA}_${PROC}_$(date +%G%m%d).log
	
	mv -f ${SALIDA}/imp_${TABLA}_${PROC}_$(date +%G%m%d).log  ${EXP_DIR}
	mv -f ${SALIDA}/`basename ${FILEDMP}` ${EXP_DIR}
	gzip  ${FILEDMP} &
fi

echo "
WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK
SET SERVEROUTPUT ON SIZE 1000000;
SET FEEDBACK OFF;


conn ${CONN}
	
declare
v_count pls_integer:=0;

begin

select count(*)
  into v_count
from ${TABLA};
	
update pbtb_procesos_imports
   SET COUNT_FIN=v_count
  where tabla = '${TABLA}'
    AND fase = '${PREF}'
	AND paso = '${PROC}'
	and fecha_mdg = '${FECHA_MDG}'	
	AND fecha = '$FECHA';
commit;
end;
/

declare
v_tabla pbtb_procesos_exports.tabla%type;

begin

 select   tabla  
 into v_tabla
 from pbtb_procesos_imports
  where tabla = '${TABLA}'
    AND fase = '${PREF}'
	AND paso = '${PROC}'
	AND fecha = '$FECHA'
	and fecha_mdg = '${FECHA_MDG}'	
	AND count_ini=count_fin;
exception
when no_data_found then

dbms_output.put_line('UDE-00000. Error de integridad de export. Error en la fase ${FASE} paso ${PASO} en la tabla ${TABLA}');
raise;
end;
/


exit" >> tmp_update_imp_${TABLA}.sql

sqlplus  /nolog @tmp_update_imp_${TABLA}.sql 2> ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_UPDATE_${FECHA}.log
if [ $? -ne 0 ];
then
   echo "Error en la fase ${PREF} paso ${PASO} en la tabla ${TABLA}'";
   cat ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_UPDATE_${FECHA}.log
   exit 1
fi  

rm tmp_update_imp_${TABLA}.sql
exit

