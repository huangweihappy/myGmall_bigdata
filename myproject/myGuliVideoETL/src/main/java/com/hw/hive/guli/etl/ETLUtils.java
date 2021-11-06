package com.hw.hive.guli.etl;

public class ETLUtils {

    /**
     * 清洗视频数据
     *
     * 规则:
     * 1. 数据长度必须大于等于9
     * 2. 将视频的类别中的空格去掉
     * 3. 将关联视频通过&拼接



     * @param line
     * @return 如果数据合法，返回清洗完的数据
     *         如果数据不合法， 返回null
     *
     * 测试数据:
     * RX24KLBhwMI	lemonette	697	People & Blogs	512	24149	4.22	315	474	t60tW0WevkE	WZgoejVDZlo	Xa_op4MhSkg	MwynZ8qTwXA	sfG2rtAkAcg	j72VLPwzd_c	24Qfs69Al3U	EGWutOjVx4M	KVkseZR5coU	R6OaRcsfnY4	dGM3k_4cNhE	ai-cSq6APLQ	73M0y-iD9WE	3uKOSjE79YA	9BBu5N0iFBg	7f9zwx52xgA	ncEV0tSC7xM	H-J8Kbx9o68	s8xf4QX1UvA	2cKd9ERh5-8
     */
    public static String etlGulivideoData(String line){
        StringBuffer sbs = new StringBuffer();
        //1. 切割数据
        String[] splits = line.split("\t");
        //2. 规则一
        if(splits.length < 9) {
            return null ;
        }
        //3. 规则二
        splits[3] = splits[3].replaceAll(" ","");

        //4. 规则三
        for (int i = 0; i < splits.length; i++) {
            // 有相关视频 或者 没有相关视频
            if(i <=8){
                if(i == splits.length-1){
                    sbs.append(splits[i]);
                }else{
                    sbs.append(splits[i]).append("\t");
                }
            }else{
                if(i == splits.length-1){
                    sbs.append(splits[i]);
                }else{
                    sbs.append(splits[i]).append("&");
                }
            }
        }

        return sbs.toString() ;

    }

    public static void main(String[] args) {
        String line = "RX24KLBhwMI\tlemonette\t697\tPeople & Blogs\t512\t24149\t4.22\t315" ;
        String result = etlGulivideoData(line);
        System.out.println(result);
    }
}
