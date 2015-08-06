#!/bin/sh

jar_path="."
jar_name="od-assembly-1.0.jar"

#ods
ODS_Cell_OD_Track_indir="odsin"
ODS_Cell_OD_Track_outdir="odsout"
#dw
DW_City_OD_Track_outdir="dwcityout"
DW_County_OD_Track_outdir="dwcountryout"


ODS_Cell_OD_Track(){
	echo "hadoop jar ${jar_path}/${jar_name} com.boco.od.ods.CellOdTraceDriver ${ODS_Cell_OD_Track_indir} ${ODS_Cell_OD_Track_outdir}"
    hadoop jar ${jar_path}/${jar_name} com.boco.od.ods.CellOdTraceDriver ${ODS_Cell_OD_Track_indir} ${ODS_Cell_OD_Track_outdir}
}

DW_City_OD_Track(){
	inputdir=${ODS_Cell_OD_Track_outdir}
	echo "hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LocationOdDriver $inputdir ${DW_City_OD_Track_outdir} city gbk"
	hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LocationOdDriver $inputdir ${DW_City_OD_Track_outdir} city gbk
}

DW_County_OD_Track(){
	inputdir=${ODS_Cell_OD_Track_outdir}
	echo "hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LocationOdDriver $inputdir ${DW_County_OD_Track_outdir} region gbk"
	hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LocationOdDriver $inputdir ${DW_County_OD_Track_outdir} region gbk
}
#TODO
DW_Live_Work_15Day(){
    inputdir=${ODS_Cell_OD_Track_outdir}
	echo "hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LocationOdDriver $inputdir ${DW_County_OD_Track_outdir} region gbk"
	#hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LocationOdDriver $inputdir ${DW_County_OD_Track_outdir} region gbk

}

case "$1" in
	'ods')
	#nohup java $JAVA_OPTS -cp $JARS com.boco.sysadmin.app.SysadminApp $1 >> ${COMM_HOME}/bin/nohup.out &
	#echo $! >> pidfile
	echo "step => calc ODS_Cell_OD_Track"
	ODS_Cell_OD_Track
	;;
	'dwcity')
	#nohup java $JAVA_OPTS -cp $JARS com.boco.sysadmin.app.SysadminApp $2 >> ${COMM_HOME}/bin/nohup.out &
	echo "step => calc DW_City_OD_Track"
	DW_City_OD_Track
	;;
   'dwregion')
	#nohup java $JAVA_OPTS -cp $JARS com.boco.sysadmin.app.SysadminApp $2 >> ${COMM_HOME}/bin/nohup.out &
	echo "step => calc DW_County_OD_Track"
	DW_County_OD_Track
	;;
	'dw15d')
	#rm ${COMM_HOME}/log/*
	#rm ${COMM_HOME}/bin/nohup.out
	#echo "stop finished"
	#ps -ef | grep sysadmin|grep -v grep|awk '{print $2}'|xargs -i kill -9 {}
    echo "step => calc DW_Live_Work_15Day"
    DW_Live_Work_15Day
	;;
	*)
	echo "ltod , boco ltd. (c)2010-2015"
	echo "usage: ./od.sh <command>"
	echo "       command = < ods | dwcity | dwregion | dw15d |help >"
	echo "       ods: calc ODS_Cell_OD_Track."
	echo "       dwcity: calc DW_City_OD_Track."
    echo "       dwregion: calc DW_County_OD_Track."
    echo "       dw15d: calc DW_Live_Work_15Day."
	echo "       help:  help information."
	;;
esac