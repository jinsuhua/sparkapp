package com.maxus.sparksql.util;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaUtils{
    public static String fillParamWithArgs(String param, Object[] args) throws Exception{
        Pattern pattern = Pattern.compile("\\{(\\d+)\\}");
        Matcher matcher = pattern.matcher(param);
        List<Integer> ll = new ArrayList();
        while (matcher.find()){
            ll.add(Integer.parseInt(matcher.group(1)));
        }
        if (ll.size()>0) {
            int max = ll.stream().max(Integer::compareTo).get();
            if(max + 1 != args.length){
                throw new Exception("所需参数个数不匹配: "+param);
            }
        }

        return MessageFormat.format(param, args);
    }

}
