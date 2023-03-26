package com.mobil.etler;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mobil.constant.DBINFO;
import com.mobil.db.CockroachDBFactory;
import com.mobil.db.Interface.IDataSource;
import com.mobil.db.OracleDBFactory;
import com.mobil.db.batch.InsertBatch;
import com.mobil.db.batchBuilder.IBatch;
import com.mobil.db.batchBuilder.OracleBatchBuilder;
import com.mobil.db.object.Column;
import com.mobil.param.RunParam;
import com.mobil.param.ETLParam;
import com.mobil.type.BATCH_TYPE;
import com.mobil.type.LoadType;
import com.mobil.type.LoadTypeHelper;
import com.mobil.util.DataConverter;
import com.mobil.util.DateUtil;
import com.mobil.util.EasyLogger;
import com.mongodb.DB;
import sun.util.resources.ru.CalendarData_ru;

import java.sql.*;
import java.util.HashMap;
import java.util.Vector;

/**
 * Created by user on 2018-03-22.
 * Author: moonsun kim
 */
public class Cockroach {

    private OracleDBFactory _odf = null;
    private CockroachDBFactory _cdf = null;

    public Cockroach(OracleDBFactory odf, CockroachDBFactory cdf) {
        _odf = odf;
        _cdf = cdf;
    }

    public boolean runEtl(IDataSource _etlSource, RunParam runParam) throws Exception {

        Vector<ETLParam> vecEtlParams;

        vecEtlParams = _etlSource.getEtlParams(runParam);

        int nRows;

        for(ETLParam etlParam: vecEtlParams) {

            //System.out.println(" exec_sql:" + etlParam.getExecSql() );

            nRows = doEtl(_etlSource, runParam, etlParam);

            if( nRows != -1) {
                System.out.printf("ETL SUCC: OBJ_ID=[%s, %d rows] \n", etlParam.getObjId(), nRows);
                insertEtlLog(etlParam.getObjId(), nRows, runParam);
            } else {
                System.out.printf("ETL FAIL: OBJ_ID=[%s] \n", etlParam.getObjId());
                return false;
            }

            try {
                   Thread.sleep(1000 * 10);
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        return true;
    }

    private int doEtl(IDataSource _etlSource, RunParam runParam, ETLParam etlParam) throws Exception {

        int nRows = 0;
        int nKeyIndex = 0;

        PreparedStatement pstmt = null;
        ResultSet rset = null;
        IBatch iBatch = null;

        String strTagetTableName = runParam.getStrTargetDName() + "." + etlParam.getTargetTableNm();

        try {

            LoadType loadType = LoadTypeHelper.getLoadTypeByName(etlParam.getLoadType());

            if (LoadType.TruncateAndInsert == loadType) {
                System.out.println("TruncateAndInsert Start!");

                if (etlParam.getTargetTableNm().startsWith("tn_mc_trd")) {
                    throw new Exception("table name [tn_mc_trd] start not allowed drop option! ");
                }

                if(!_cdf.truncateWithRetry(strTagetTableName)) {
                    throw new Exception(strTagetTableName + " truncate table fail!");
                }

                EasyLogger.info(etlParam.getTargetTableNm() + " Truncated.");
                System.out.println(etlParam.getTargetTableNm() + " Truncated.");

            } else if (LoadType.DeleteAndInsert == loadType) {
                System.out.println("DeleteAndInsert Start!");
                System.out.println("Delete Condition = " + etlParam.getLoadCondClause());

                long nDeletedRows = _cdf.deleteWithRetry(strTagetTableName, etlParam.getLoadCondClause());

                if (nDeletedRows == -1)
                    return -1;

                EasyLogger.info(etlParam.getTargetTableNm() + " deleted rows = " + nDeletedRows);
                System.out.println(etlParam.getTargetTableNm() + " deleted rows = " + nDeletedRows);

            } else if (LoadType.UpdateOnly == loadType) {
                System.out.println("UpdateOnly Start!");

                String keyIndex = etlParam.getLoadCondClause();

                if (keyIndex == null || keyIndex.trim().length() == 0) {
                    throw new Exception("LoadType[UpdateOnly] keyIndex is null");
                }

                nKeyIndex = Integer.parseInt(keyIndex);

            } else if (LoadType.InsertOnly == loadType) {
                System.out.println("INSERT ONLY");
            } else {
                System.out.println("Error: Not Defined ETLAction = " + loadType.name());
                return -1;
            }


            pstmt  = ((Connection)_odf.getConnection()).prepareStatement(etlParam.getExecSql());
            pstmt.setFetchSize(DBINFO.MAX_SIZE);

            System.out.println(etlParam.getExecSql());

            rset = pstmt.executeQuery();

            ResultSetMetaData rmeta = rset.getMetaData();
            Vector<Column> _columnVec = DataConverter.getColumnsFromMeta(rmeta);

            iBatch = (new OracleBatchBuilder()).doBatchBuild(BATCH_TYPE.INSERT, (java.sql.Connection)_cdf.getConnection(), strTagetTableName, _columnVec);

            nRows = iBatch.runBatch(rset, DBINFO.MAX_SIZE);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if(rset  != null) try { rset.close();  } catch (Exception ig){ ig.printStackTrace();}
            if(pstmt != null) try { pstmt.close(); } catch (Exception ig){ ig.printStackTrace();}
        }

        return nRows;
    }


    private boolean insertEtlLog(String objID, int nRows, RunParam param) {

        JsonArray  data = new JsonArray();
        JsonObject jo = new JsonObject();

        try {

            jo.addProperty("log_dtm", DateUtil.getCurrentDtm());
            jo.addProperty("obj_id", objID);
            jo.addProperty("proc_cnt", nRows);
            jo.addProperty("start_dt", param.getStrStDt());
            jo.addProperty("end_dt", param.getStrEdDt());

            data.add(jo);

//            _cdf.insertMany("tn_etl_log", data);

        } catch(Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }


}

