package com.mobil.param;

/**
 * Created by user on 2018-04-05.
 */
public class ETLParam {

    private String objId;
    private String execSql;
    private String targetTableNm;
    private String loadType;
    private String loadCondClause;
    private String ymd;

    public String getObjId() { return objId; }

    public void setObjId(String objId) { this.objId = objId; }

    public String getExecSql() {
        return execSql;
    }

    public void setExecSql(String execSql) {
        this.execSql = execSql;
    }

    public String getTargetTableNm() { return targetTableNm;}

    public void setTargetTableNm(String targetTableNm) { this.targetTableNm = targetTableNm; }

    public String getLoadType() {
        return loadType;
    }

    public void setLoadType(String loadType) {
        this.loadType = loadType;
    }

    public String getLoadCondClause() {
        return loadCondClause;
    }

    public void setLoadCondClause(String loadCondClause) {
        this.loadCondClause = loadCondClause;
    }

    public String getYmd() {return ymd;}

    public void setYmd(String ymd) { this.ymd = ymd; }

}
