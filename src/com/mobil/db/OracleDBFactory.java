package com.mobil.db;

import java.sql.*;
import java.util.Vector;

import com.mobil.db.Interface.IDataSource;
import com.mobil.param.ETLParam;
import com.mobil.param.IndexCmdParam;
import com.mobil.param.RunParam;
import com.mobil.util.DataUtil;
import com.mobil.util.EasyLogger;


/**
 * Created by user on 2018-03-19.
 * Author: moonsun kim
 */
public class OracleDBFactory implements IDataSource {

    private Connection _con = null;

    @Override
    public boolean connect(String dbName, String userId, String userPasswd) {

        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

            _con = DriverManager.getConnection("jdbc:oracle:thin:@1.1.1.1:15005/" + dbName, userId, userPasswd);

            if(_con != null) {
                System.out.println("Oracle Connect Success");
            } else {
                System.out.println("Oracle Connect Fail");
            }

        } catch(Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public boolean disConnect() {

        if (_con != null) {

            try {
                _con.close();
            } catch(Exception e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }

    @Override
    public Object getConnection() {
        return _con;
    }

    public void commit() throws Exception {
        _con.commit();
    }

    public void rollback() throws Exception {
        _con.rollback();
    }

    @Override
    public Vector<ETLParam> getEtlParams(RunParam _runParam) throws Exception {

        Vector<ETLParam> vecEtlParams = new Vector<>();

        StringBuilder sbQry = new StringBuilder();
        sbQry.append("SELECT OBJ_ID                                                              \n");
        sbQry.append("      ,EXEC_SQL1 || EXEC_SQL2 || EXEC_SQL3  AS EXEC_SQL                    \n");
        sbQry.append("      ,MONGO_DB_NM                                                         \n");
        sbQry.append("      ,MONGO_CLT_NM                                                        \n");
        sbQry.append("      ,LOAD_TYPE                                                           \n");
        sbQry.append("      ,LOAD_COND_CLAUSE                                                    \n");
        sbQry.append("FROM MORMS.TB_ETL_OBJECT                                                   \n");
        sbQry.append("WHERE USE_YN = 'Y'                                                         \n");
        sbQry.append("  AND ETL_MODE = '").append(_runParam.getEtlMode().toString()).append("'   \n");
        if (_runParam.getStrObjId() != null && _runParam.getStrObjId().length() > 0) {
            sbQry.append("AND OBJ_ID = '").append(_runParam.getStrObjId()).append("'             \n");
        }
        sbQry.append("ORDER BY EXEC_SEQ_NO                                                       \n");

        System.out.println("sql = " + sbQry.toString());

        Statement stmt = null;
        ResultSet rset = null;
        ETLParam etlParam;

        try {
            stmt = _con.createStatement();
            rset = stmt.executeQuery(sbQry.toString());

            String strExecSql;
            String strCondJson;

            while (rset.next()) {

                // SQL에 미리 정의된 문자열 치환 - 시작일자, 종료일자

                strExecSql = DataUtil.getNullData(rset.getString("EXEC_SQL"));
                strExecSql = strExecSql.replaceAll("[{]START_DT[}]", _runParam.getStrStDt());
                strExecSql = strExecSql.replaceAll("[{]END_DT[}]", _runParam.getStrEdDt());

                strCondJson = DataUtil.getNullData(rset.getString("LOAD_COND_CLAUSE"));
                strCondJson = strCondJson.replaceAll("[{]START_DT[}]", _runParam.getStrStDt());
                strCondJson = strCondJson.replaceAll("[{]END_DT[}]", _runParam.getStrEdDt());

                etlParam = new ETLParam();

                etlParam.setYmd(_runParam.getStrStDt().substring(0, 6));
                etlParam.setExecSql(strExecSql);
                etlParam.setLoadCondClause(strCondJson);
                etlParam.setObjId(rset.getString("OBJ_ID"));
                etlParam.setTargetTableNm(rset.getString("MONGO_CLT_NM").replaceAll("[{]YYYYMM[}]", etlParam.getYmd()));
                etlParam.setLoadType(rset.getString("LOAD_TYPE"));

                vecEtlParams.add(etlParam);

            }

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if(rset  != null) try { rset.close();  } catch (Exception ig){ ig.printStackTrace();}
            if(stmt != null) try { stmt.close(); } catch (Exception ig){ ig.printStackTrace();}
        }

        return vecEtlParams;
    }

    @Override
    public Vector<IndexCmdParam> getIndexCommands(ETLParam _etlParam) throws Exception {

        Vector<IndexCmdParam> vecIdxCmds = new Vector<>();

        StringBuilder sbQry = new StringBuilder();
        sbQry.append(" SELECT OBJ_ID              \n");
        sbQry.append("       ,SEQ_NO              \n");
        sbQry.append("       ,EXEC_CODE           \n");
        sbQry.append(" FROM MORMS.TB_ETL_AFT_CODE \n");
        sbQry.append(" WHERE USE_YN = 'Y'         \n");
        sbQry.append("   AND OBJ_ID = ?           \n");
        sbQry.append(" ORDER BY SEQ_NO            \n");

        System.out.println("sql = " + sbQry.toString());

        PreparedStatement pstmt = null;
        ResultSet         rset  = null;

        try {

            pstmt = _con.prepareStatement(sbQry.toString());
            pstmt.setString(1, _etlParam.getObjId());

            rset = pstmt.executeQuery();

            String strExecCode;
            IndexCmdParam idxParam;

            while (rset.next()) {

                idxParam = new IndexCmdParam();

                strExecCode = rset.getString("EXEC_CODE");
                strExecCode = strExecCode.replaceAll("[{]YYYYMM[}]", _etlParam.getYmd());

                idxParam.setSeqNo(rset.getInt("SEQ_NO"));
                idxParam.setStrCommand(strExecCode);

                System.out.printf("runCommand = [%s] \n", strExecCode);

                vecIdxCmds.add(idxParam);

            }

        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            if(rset  != null) try { rset.close();  } catch (Exception ig){ ig.printStackTrace();}
            if(pstmt != null) try { pstmt.close(); } catch (Exception ig){ ig.printStackTrace();}
        }

        return vecIdxCmds;
    }

}
