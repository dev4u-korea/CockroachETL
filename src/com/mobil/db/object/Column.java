package com.mobil.db.object;

import com.mobil.type.DATA_TYPE;

/**
 * Created by user on 2019-03-15.
 */
public class Column {

    private String columnName;
    private String dataValue;
    private DATA_TYPE dataType;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public DATA_TYPE getDataType() {
        return dataType;
    }

    public void setDataType(DATA_TYPE dataType) {
        this.dataType = dataType;
    }

    public String getDataValue() {
        return dataValue;
    }

    public void setDataValue(String dataValue) {
        this.dataValue = dataValue;
    }

}
