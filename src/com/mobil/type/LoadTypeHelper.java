package com.mobil.type;

/**
 * Created by user on 2018-04-05.
 */
public class LoadTypeHelper {

    public static LoadType getLoadTypeByName(String strLoadType) throws Exception {

        if (LoadType.InsertOnly.name().equals(strLoadType)) {
            return LoadType.InsertOnly;
        } else if (LoadType.DeleteAndInsert.name().equals(strLoadType)) {
            return LoadType.DeleteAndInsert;
        } else if (LoadType.TruncateAndInsert.name().equals(strLoadType)) {
            return LoadType.TruncateAndInsert;
        } else if (LoadType.UpdateOnly.name().equals(strLoadType)) {
            return LoadType.UpdateOnly;
        } else {
            throw new Exception("Not Matched LoadType!");
        }
    }
}
