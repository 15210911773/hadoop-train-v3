package com.bigdata.hadoop.mr.project.v1.mr;

import com.bigdata.hadoop.mr.project.ip.IPLocation;
import com.bigdata.hadoop.mr.project.ip.IPSeeker;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jizhe.pan on 2019-04-08
 */
public class LogParser {

    public Map<String, String> parse(String log) {
        Map<String, String> info = new HashMap<>();
        if (StringUtils.isNotBlank(log)) {
            String[] s = log.split(" ");
            String ip = s[1];

            IPSeeker ipSeeker = new IPSeeker("qqwry.dat", "ip");
            IPLocation ipLocation = ipSeeker.getIPLocation(ip);
            if (ipLocation != null) {
                info.put("province", ipLocation.getCountry());
                info.put("area", ipLocation.getArea());
            }
            info.put("ip", ip);
        }
        return info;
    }

}
