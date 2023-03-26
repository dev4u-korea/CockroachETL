package com.mobil.param;


import com.mobil.constant.DBINFO;
import com.mobil.type.ETLModeType;

/**
 * Created by user on 2018-04-02.
 */
public class RunParam {

    private ETLModeType etlMode = ETLModeType.A;
    private String strStDt  = "";
    private String strEdDt  = "";
    private String strObjId  = "";
    private String strTargetDName = DBINFO.MOBDW_DB_NM;
    private String strTargetUName = DBINFO.MOBDW_DB_ID;
    private String strTargetUPass = DBINFO.MOBDW_DB_PWD;

    public ETLModeType getEtlMode() { return etlMode; }

    public void setEtlMode(ETLModeType etlMode) {this.etlMode = etlMode;}

    public String getStrStDt() {
        return strStDt;
    }

    public void setStrStDt(String strStDt) {
        this.strStDt = strStDt;
    }

    public String getStrEdDt() {
        return strEdDt;
    }

    public void setStrEdDt(String strEdDt) {
        this.strEdDt = strEdDt;
    }

    public String getStrObjId() {
        return strObjId;
    }

    public void setStrObjId(String strObjId) {
        this.strObjId = strObjId;
    }

    public String getStrTargetDName() { return strTargetDName; }

    public void setStrTargetDName(String strTargetDName) { this.strTargetDName = strTargetDName; }

    public String getStrTargetUName() { return strTargetUName; }

    public void setStrTargetUName(String strTargetUName) { this.strTargetUName = strTargetUName; }

    public String getStrTargetUPass() { return strTargetUPass; }

    public void setStrTargetUPass(String strTargetUPass) { this.strTargetUPass = strTargetUPass; }

    public String getString() {
        String str = "";

        str = String.format("EtlMode = [%s], StartDT = [%s], EndDT = [%s], ObjID[%s], TargetDB[%s]", etlMode.toString(), strStDt, strEdDt, strObjId, strTargetDName);

        return str;
    }

}
