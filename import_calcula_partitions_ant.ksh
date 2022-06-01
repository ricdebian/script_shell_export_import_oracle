# Script que realiza la importación de tablas particionadas o no.
# En el parametro 1 se especifica el nombre de la tabla a importar
# En el parametro 2 se especifica el paso en el que se realizó el export del fichero que queremos importar.
# En el parametro 3 se especifica la fecha de exportación del fichero que queremos importar.

######
# Función que comprueba errores en los logs 
######
comprueba_error(){
	    echo "Comprobando error..."
		cat $1 | grep "ORA-" 2>&1 > /dev/null
		ORA=$?
		cat $1 | grep "IMP-" 2>&1 > /dev/null
		IMP=$?
		echo "$ORA $IMP" ;
	    # Si es igual a 0 es porque el grep a encontrado el patrón que busca errores. 
		# Si es igual a 1 el grep no ha encontrado nada con el patrón
		if [[ $ORA -eq 0 || $IMP -eq 0 ]] 
	    then
	    # Si se ha generado un error se muestra el error y salgo.
	     echo "Error en el fichero $1";
		 cat $1 | grep "ORA-";
		 cat $1 | grep "IMP-";
	      exit 1;
	    fi		
	}

TABLA=$1
PROC=$2
FECHA=$3


. /app/EXPL/E592/entrada/scripts/MDG_MENSUAL/.sesion.dat


echo "###################################################"
echo "##        Ejecución en $ORACLE_SID       		   ##"
echo "##  Con los usuarios ${ORA_USU_A} y ${ORA_USU_E} ##" 
echo "###################################################"



# Prefijo utilizado por sistemas en la copia masiva de fichero para backups.
PREF="AH"


EXP_DIR="/app/EXPL/E592/exports/ejecuciones/MDG_MENSUAL/ASIGNACION_HUECO_VP/$ORACLE_SID/$PROC/$FECHA"

DIR=`pwd`
echo `basename $DIR`
#exit 1

i=0

CONN="$ORA_USU_E/$ORA_PWD_E@${ORACLE_SID}"

echo  "Ficheros a importar: ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_*${FECHA}.dmp.gz";

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
 echo "No existen fichero dmp de importación para la tabla $TABLA en esta fase";
 exit 1
fi


echo "Nº de particiones a importar $i";

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


sqlplus  /nolog @temp${TABLA}.sql ${TABLA}

sqlplus /nolog @trunc_drop_index${TABLA}.sql
echo "Borro script temporales."
rm trunc_drop_index${TABLA}.sql
rm temp${TABLA}.sql
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
	 echo ${CONN} | imp tables=${TABLA}:${PART} file=${FILEDMP} log=${EXP_DIR}/imp_${TABLA}_${PROC}_${PART}_$(date +%G%m%d).log ignore=y indexes=y 1>/dev/null
	
	 gzip ${FILEDMP} &
	 
	done
	
else	

	echo "No tiene particiones"
	gunzip  ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_${FECHA}.dmp.gz
	FILEDMP=`ls ${EXP_DIR}/${PREF}_${TABLA}_${PROC}_${FECHA}.dmp`
#	imp userid=${CONN} tables=${TABLA} file=${FILEDMP} log=${EXP_DIR}/imp_${TABLA}_$(date +%G%m%d).log ignore=y indexes=y
    echo ${CONN} | imp tables=${TABLA} file=${FILEDMP} log=${EXP_DIR}/imp_${TABLA}_${PROC}_$(date +%G%m%d).log ignore=y indexes=y 1>/dev/null
	gzip $FILEDMP	&
 
fi

