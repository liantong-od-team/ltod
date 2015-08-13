#!/bin/sh

jar_path="."
jar_name="od-assembly-1.0.jar"

#ods
ODS_Cell_OD_Track_indir="hdfs://boh/user/hive/warehouse/zba_dwa.db/zb_d_yd_voice_013/*/*/*/*/*"
ODS_Cell_OD_Track_outdir="/sunwukongtmp/odtest/autorun/odoutl1"
ODS_Cell_OD_Track_outdir2="/sunwukongtmp/odtest/autorun/odoutl2"

dim_cell="/sunwukongtmp/dim_all_station_cellid_new/"
dim_lrc="hdfs://boh/user/hive/warehouse/zba_dwa.db/dim_msisdn_seg/"

#ODS_Cell_OD_Track_indir="fy/lt/table"
#ODS_Cell_OD_Track_outdir="fy/lt/autorun/odsoutl1"
#ODS_Cell_OD_Track_outdir2="fy/lt/autorun/odsoutl2"
#
#dim_cell="fy/lt/dim/dim_all_station_cellid_new"
#dim_lrc="fy/lt/dim/dim_msisdn_seg"




#dw
DW_City_OD_Track_outdir="/sunwukongtmp/odtest/autorun/dwcityout"
DW_County_OD_Track_outdir="/sunwukongtmp/odtest/autorun/dwcountryout"

#DW_City_OD_Track_outdir="fy/lt/autorun/dwcityout"
#DW_County_OD_Track_outdir="fy/lt/autorun/dwcountryout"


ODS_Cell_OD_Track(){
	echo "hadoop jar ${jar_path}/${jar_name} com.boco.od.ods.CellOdTraceDriverLocal ${ODS_Cell_OD_Track_indir} ${ODS_Cell_OD_Track_outdir} ${dim_cell} ${dim_lrc} dim_all_station_cellid_new DIM_MSISDN_SEG"
    hadoop jar ${jar_path}/${jar_name} com.boco.od.ods.CellOdTraceDriverLocal ${ODS_Cell_OD_Track_indir} ${ODS_Cell_OD_Track_outdir} ${dim_cell} ${dim_lrc} dim_all_station_cellid_new DIM_MSISDN_SEG

    echo "hadoop jar ${jar_path}/${jar_name} com.boco.od.ods.XDriver ${ODS_Cell_OD_Track_outdir} ${ODS_Cell_OD_Track_outdir2}"
    hadoop jar ${jar_path}/${jar_name} com.boco.od.ods.XDriver ${ODS_Cell_OD_Track_outdir} ${ODS_Cell_OD_Track_outdir2}


}

DW_City_OD_Track(){
	inputdir=${ODS_Cell_OD_Track_outdir2}
	echo "hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LocationOdDriver $inputdir ${DW_City_OD_Track_outdir} city gbk"
	hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LocationOdDriver $inputdir ${DW_City_OD_Track_outdir} city gbk
}

DW_County_OD_Track(){
	inputdir=${ODS_Cell_OD_Track_outdir2}
	echo "hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LocationOdDriver $inputdir ${DW_County_OD_Track_outdir} region gbk"
	hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LocationOdDriver $inputdir ${DW_County_OD_Track_outdir} region gbk
}

DW_Live_Work_15Day(){
    shift
	echo "hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LiveWorkDriver $*"
	hadoop jar ${jar_path}/${jar_name} com.boco.od.location.LiveWorkDriver $*

}

case "$1" in
	'ods')

	echo "step => calc ODS_Cell_OD_Track"
	ODS_Cell_OD_Track
	;;
	'dwcity')
	echo "step => calc DW_City_OD_Track"
	DW_City_OD_Track
	;;
   'dwregion')
	echo "step => calc DW_County_OD_Track"
	DW_County_OD_Track
	;;
	'dw15d')
    echo "step => calc DW_Live_Work_15Day"
    DW_Live_Work_15Day $*

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