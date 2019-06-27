package cn.ipanel.utils;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 推送消息工具类
 */
public class PushTools {
    private static Logger logger = LoggerFactory.getLogger(PushTools.class);

    /**
     * 发送post消息
     * @param reqURL url
     * @param data 传输的数据
     * @return
     */
    public static String sendPostRequest(String reqURL, String data) {
        HttpPost httpPost = new HttpPost(reqURL);
//        HttpGet httpPost = new HttpGet(reqURL);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        String result = "";
        CloseableHttpResponse response = null;
        try {
//            httpPost.setHeader("Authorization", authorization);
            httpPost.setHeader(HTTP.CONTENT_TYPE, "application/json");

            StringEntity stringEntity = new StringEntity(data, "UTF-8");
            stringEntity.setContentEncoding(new BasicHeader(HTTP.CONTENT_ENCODING, "UTF-8"));
            stringEntity.setContentType("application/json");

            httpPost.setEntity(stringEntity);
            response = httpClient.execute(httpPost);
            HttpEntity httpEntity = response.getEntity();
            if (httpEntity != null) {
                result = EntityUtils.toString(httpEntity, "UTF-8");
                EntityUtils.consume(httpEntity);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("请求通信[" + reqURL + "]时偶遇异常,堆栈轨迹如下", e);
        } finally {
            closeHttp(response, httpClient);
        }
        return result;
    }

    private static void closeHttp(CloseableHttpResponse response, CloseableHttpClient httpClient) {
        try {
            if (response != null) {
                response.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            if (httpClient != null) {
                httpClient.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 发送get请求
     * @param reqURL url
     * @param params 入参
     */
    public static String sendGetRequest(String reqURL, Map<String, Object> params) {
        String result = null; // 响应内容
        CloseableHttpResponse response = null;
        CloseableHttpClient httpClient = HttpClients.createDefault(); // 创建默认的httpClient实例
        try {
            if (params != null && !params.isEmpty()) {

                List<NameValuePair> pairs = new ArrayList<NameValuePair>(params.size());

                for (String key : params.keySet()) {
                    pairs.add(new BasicNameValuePair(key, params.get(key).toString()));
                }
                reqURL += "?" + EntityUtils.toString(new UrlEncodedFormEntity(pairs), "UTF-8");
            }
            HttpGet httpGet = new HttpGet(reqURL); // 创建org.apache.http.client.methods.HttpGet
//            httpGet.setHeader("Authorization", auth);
            httpGet.setHeader(HTTP.CONTENT_TYPE, "application/json");

            response = httpClient.execute(httpGet); // 执行GET请求
            HttpEntity httpEntity = response.getEntity(); // 获取响应实体
            if (null != httpEntity) {
                result = EntityUtils.toString(httpEntity, "UTF-8");
                System.out.println(result);
                EntityUtils.consume(httpEntity);
            }
            response.close();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("异常信息：", e);
        } finally {
            closeHttp(response, httpClient);
        }
        return result;
    }


}
