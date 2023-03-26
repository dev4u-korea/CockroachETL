package com.mobil.main;

import com.mobil.constant.DBINFO;
import com.mobil.db.Interface.IDataSource;
import com.mobil.db.CockroachDBFactory;
import com.mobil.db.OracleDBFactory;
import com.mobil.param.RunParam;
import com.mobil.type.ETLModeType;
import com.mobil.type.ETLSourceType;
import com.mobil.util.EasyLogger;
import com.mobil.etler.Cockroach;


/**
 * Created by user on 2018-03-09.
 * Author: moonsun kim
 */
public class CockroachETL {

    private OracleDBFactory _odf         = null;
    private IDataSource     _etlSource   = null;
    private CockroachDBFactory  _cdf     = null;
    private RunParam        _runParam    = null;
    private final ETLSourceType _etlSourceType = ETLSourceType.MOBDW;

    private void start() {
        EasyLogger.info("CockroachETL Start!");
    }

    private void end() {
        EasyLogger.info("CockroachETL End!");
    }

    private  boolean openDataSource() throws Exception {

        if (_etlSourceType == ETLSourceType.PBILL) {
            _etlSource = new OracleDBFactory();
            if (_etlSource.connect(DBINFO.ORACLE_DB_NM, DBINFO.ORACLE_DB_ID, DBINFO.ORACLE_DB_PWD)) {
                EasyLogger.info("OpenDataSource: Oracle ETL Open");
            } else {
                return false;
            }
        } else  if (_etlSourceType == ETLSourceType.MOBDW){
            _etlSource = new CockroachDBFactory();
            if (_etlSource.connect(DBINFO.MOBDW_DB_NM, DBINFO.MOBDW_DB_ID, DBINFO.MOBDW_DB_PWD)) {
                EasyLogger.info("OpenDataSource: CockroachDB ETL Open");
            } else {
                return false;
            }
        } else {
            System.out.println(">> Error. Undefined ETLSourceType : " + _etlSourceType.toString());
            return false;
        }

        _odf = new OracleDBFactory();

        if(_odf.connect(DBINFO.ORACLE_DB_NM, DBINFO.ORACLE_DB_ID, DBINFO.ORACLE_DB_PWD)) {
            EasyLogger.info("OpenDataSource: Oracle Open");
        } else {
            return false;
        }

        _cdf = new CockroachDBFactory();

        if(_cdf.connect(_runParam.getStrTargetDName(), _runParam.getStrTargetUName(), _runParam.getStrTargetUPass())) {
            EasyLogger.info("OpenDataSource: CockroachDB Open => " + _runParam.getStrTargetDName());
        } else {
            return false;
        }

        
        return true;
    }

    private boolean closeDataSource() {

        boolean bRet = true;

        if(_odf != null) {
            try {
                if(_odf.disConnect()) {
                    EasyLogger.info("Close DataSource: Oracle Close");
                }
            } catch (Exception e) {
                e.printStackTrace();
                bRet = false;
            }
        }

        if (_etlSourceType == ETLSourceType.PBILL) {
            if(_etlSource != null) {
                try {
                    if (_etlSource.disConnect()) {
                        EasyLogger.info("Close DataSource: Oracle ETL Close");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    bRet = false;
                }
            }
        } else  if (_etlSourceType == ETLSourceType.MOBDW){
            if (_etlSource != null) {
                try {
                    if (_etlSource.disConnect()) {
                        EasyLogger.info("Close DataSource: CockroachDB ETL Close");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    bRet = false;
                }
            }
        } else {
            System.out.println(">> Error. Undefined ETLSourceType : " + _etlSourceType.toString());
            return false;
        }

        if(_cdf != null) {
            try {
                if(_cdf.disConnect()) {
                    EasyLogger.info("Close DataSource: CockroachDB close >> " + _runParam.getStrTargetDName());
                }
            } catch (Exception e) {
                e.printStackTrace();
                bRet = false;
            }
        }

        return bRet;
    }

    private void run() throws Exception {

        runETL();

    }

    private void runETL() throws Exception{

        EasyLogger.info("runETL() start!");

        Cockroach cocoEtl = new Cockroach(_odf, _cdf);

        cocoEtl.runEtl(_etlSource, _runParam);

        EasyLogger.info("runETL() end!");
    }

    private void setRunParameter(String[] args) throws Exception {

        _runParam = new RunParam();

        try {
            _runParam.setEtlMode(ETLModeType.valueOf(args[0]));
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("ETL_MODE is 'A' or 'M");
        }

        _runParam.setStrStDt(args[1].trim());

        if(_runParam.getStrStDt().length() != 8) {
            throw new Exception("START_DT length is 8");
        }

        _runParam.setStrEdDt(args[2].trim());

        if(_runParam.getStrEdDt().length() != 8) {
            throw new Exception("END_DT length is 8");
        }

        if (args.length >= 4) {
            _runParam.setStrObjId(args[3].trim());
        }


        System.out.printf("ETLMode  = [%s] \n", _runParam.getEtlMode().toString());
        System.out.printf("StartDt  = [%s] \n", _runParam.getStrStDt());
        System.out.printf("EndDt    = [%s] \n", _runParam.getStrEdDt());

        if (args.length >= 4)
            System.out.printf("ObjID    = [%s] \n", _runParam.getStrObjId());

        System.out.printf("TargetDB = [%s] \n", _runParam.getStrTargetDName());

        EasyLogger.info(_runParam.getString());

    }

    private void showUsage() {
        EasyLogger.info(">> Help CockroachETL            <<");
        EasyLogger.info("Usage 1: CockroachETL [ETL_MODE] [START_DT] [END_DT]");
        EasyLogger.info("Usage 2: CockroachETL [ETL_MODE] [START_DT] [END_DT] [OBJ_ID]");
        EasyLogger.info("Usage 3: CockroachETL [ETL_MODE] [START_DT] [END_DT] [OBJ_ID] [TARGET_DB]");
        EasyLogger.info(">> ------------------------ <<");

        System.out.println(">> Help CockroachETL            <<");
        System.out.println("Usage 1: CockroachETL [ETL_MODE] [START_DT] [END_DT]");
        System.out.println("Usage 2: CockroachETL [ETL_MODE] [START_DT] [END_DT] [OBJ_ID]");
        System.out.println("Usage 3: CockroachETL [ETL_MODE] [START_DT] [END_DT] [OBJ_ID] [TARGET_DB]");
        System.out.println(">> ------------------------ <<");
    }

    private boolean checkParam() {

        if (_runParam.getEtlMode() == ETLModeType.M) {
            if(_runParam.getStrObjId() == null || _runParam.getStrObjId().trim().length() == 0) {
                System.out.printf("ETLMode[M] IS OBJ_ID REQUIRED! \n");
                EasyLogger.info("ETLMode[M] IS OBJ_ID REQUIRED!");
                return false;
            }
        }

        return true;
    }

    public static void main(String[] args) throws Exception {

        CockroachETL etl = new CockroachETL();

        /* Start Message */
        etl.start();

        /* Run Parameter Setting */
        if (args.length >= 3 && args.length <= 5) {
            etl.setRunParameter(args);
        } else {
            etl.showUsage();
            EasyLogger.info("CockroachETL Exited!");
            return;
        }

        if(!etl.checkParam()) {
            EasyLogger.info("Check Parameter Result False!");
            EasyLogger.info("CockroachETL Exited!");
            return;
        }

        /*  Open DataSource */
        if (!etl.openDataSource()) {
            EasyLogger.info("Open DataSource Fail!");
            return;
        }

        /* ETL Run */
        try { etl.run();} catch(Exception e) {e.printStackTrace();}

        /*  Close DataSource */
        if (!etl.closeDataSource()) {
            EasyLogger.info("Close DataSource Fail!");
            return;
        }

        /* End Message */
        etl.end();

    }

}

