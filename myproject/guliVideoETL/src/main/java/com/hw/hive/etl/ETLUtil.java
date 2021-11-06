package com.hw.hive.etl;

public class ETLUtil {
    /**
     * 用于数据清洗的工具类
     * @param toString
     * @return
     */
    public static String GuliVideoETL(String toString) {
        StringBuffer outk = new StringBuffer();

        String[] splits = toString.split("\t");
        if(splits.length < 9){
            return null;
        }
        splits[3]= splits[3].replaceAll(" ","");

        for (int i = 0; i < splits.length; i++) {

            if (i < 9 ){
                if(i == splits.length-1){
                    outk.append(splits[i]);
                }else{
                    outk.append(splits[i]).append("\t");
                }
            }else {
                if(i == splits.length-1){
                    outk.append(splits[i]);
                }else {
                    outk.append(splits[i]).append("&");
                }
            }
        }
        return outk.toString();
    }

}
