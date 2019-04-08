package com.bigdata.hadoop.mr.project.utils;

import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jizhe.pan on 2019-04-08
 */
public class ContentUtils {

    private static final Pattern PATTERN = Pattern.compile("topicId=[0-9]+");

    public static String getPageId(String url) {
        String pageId = "";
        if (StringUtils.isBlank(url)) {
            return pageId;
        }

        Matcher matcher = PATTERN.matcher(url);
        if (matcher.find()) {
            pageId = matcher.group().split("topicId=")[1];
        }
        return pageId;
    }

}
