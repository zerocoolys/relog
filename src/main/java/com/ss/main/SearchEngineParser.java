package com.ss.main;

import io.netty.handler.codec.http.QueryStringDecoder;

import java.util.ResourceBundle;

/**
 * Created by baizz on 2015-3-24.
 */
public class SearchEngineParser implements Constants {

//    public static final String baiduReferer = "http://www.baidu.com/s?ie=utf-8&f=8&rsv_bp=1&rsv_idx=1&tn=baidu&wd=%e8%89%ba%e9%be%99%e7%bd%91&rsv_pq=8bbec7e100005f9d&rsv_t=7e77zczzyrcbjkekvtzuovxaefeya5pz%2fvx6tkoxzj9hfazvlzxwcdxh8pa&rsv_enter=1&inputt=3460&rsv_sug3=4&rsv_sug1=3&rsv_sug2=0&rsv_sug4=3461&bs=%e8%98%91%e8%8f%87%e8%a1%97&__eis=1&__e";
//    public static final String sougouReferer = "http://www.sogou.com/web?query=%e8%89%ba%e9%be%99%e7%bd%91&_asf=www.sogou.com&_ast=&w=01019900&p=40040100&ie=utf8&sut=7841&sst0=1427169484329&lkt=0%2c0%2c0";
//    public static final String haosouReferer = "http://www.haosou.com/s?ie=utf-8&shb=1&src=360sou_newhome&q=%e8%89%ba%e9%be%99%e7%bd%91";
//    public static final String bingReferer = "http://cn.bing.com/search?q=%e8%89%ba%e9%be%99%e7%bd%91&go=%e6%8f%90%e4%ba%a4&qs=n&form=qblh&pq=%e8%89%ba%e9%be%99%e7%bd%91&sc=8-3&sp=-1&sk=&cvid=a322d834a1884c98a01ba6405852da6e";

    private static final ResourceBundle bundle = ResourceBundle.getBundle("searchEngine");


    /**
     * 根据refer解析出search engine与keyword(如果存在)
     *
     * @param refer
     * @return
     */
    public static String[] getSK(String refer) {
        String[] sk = new String[2];
        QueryStringDecoder decoder = new QueryStringDecoder(refer);
        if (refer.startsWith(bundle.getString(TypeEnum.BAIDU.getName()))) {
            sk[0] = TypeEnum.BAIDU.getName();
            sk[1] = decoder.parameters().get("wd").get(0);
            return sk;
        } else if (refer.startsWith(bundle.getString(TypeEnum.SOUGOU.getName()))) {
            sk[0] = TypeEnum.SOUGOU.getName();
            sk[1] = decoder.parameters().get("query").get(0);
            return sk;
        } else if (refer.startsWith(bundle.getString(TypeEnum.HAOSOU.getName()))) {
            sk[0] = TypeEnum.HAOSOU.getName();
            sk[1] = decoder.parameters().get("q").get(0);
            return sk;
        } else if (refer.startsWith(bundle.getString(TypeEnum.BING.getName()))) {
            sk[0] = TypeEnum.BING.getName();
            sk[1] = decoder.parameters().get("q").get(0);
            return sk;
        }

        return sk;
    }

    public static enum TypeEnum {
        BAIDU("baidu"),
        SOUGOU("sougou"),
        HAOSOU("haosou"),
        BING("bing");

        private String name;

        private TypeEnum(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
